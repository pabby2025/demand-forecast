"""Phase 1 skill normalization: cluster raw skills into canonical groups."""

import csv
import json
import re
from collections import defaultdict, Counter
from azure import OpenAI

AZURE_OPENAI_KEY = ""
AZURE_OPENAI_ENDPOINT = ""
AZURE_DEPLOYMENT = "gpt-5.2"



client = OpenAI(
    base_url=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_KEY,
)


def extract_all_skills(csv_file_path, skills_column_index=22):
    all_skills = set()

    with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # Skip header

        for row in reader:
            if len(row) > skills_column_index:
                skills_cell = row[skills_column_index]
                if not skills_cell or skills_cell.strip() == '':
                    continue

                skills = []
                comma_parts = skills_cell.split(',')
                for part in comma_parts:
                    semicolon_parts = part.split(';')
                    skills.extend(semicolon_parts)

                for skill in skills:
                    skill = skill.strip()
                    if skill:
                        all_skills.add(skill)

    return all_skills


def group_skills_by_first_letter(skills):
    groups = defaultdict(list)
    for skill in skills:
        first_char = skill[0].upper()
        if first_char.isalpha():
            groups[first_char].append(skill)
        elif first_char == '.':
            groups['.'].append(skill)
    return groups


def sanitize_variants(canonical_to_variants, allowed_skills):
    """Remove variants that are not exact matches from the allowed list."""
    cleaned = {}
    for canonical, variants in canonical_to_variants.items():
        unique_variants = list(dict.fromkeys(variants))
        valid_variants = [variant for variant in unique_variants if variant in allowed_skills]
        removed = len(unique_variants) - len(valid_variants)
        if not valid_variants and canonical in allowed_skills:
            valid_variants = [canonical]
        if removed:
            print(f"Removed {removed} invalid variants from '{canonical}'")
        if valid_variants:
            cleaned[canonical] = valid_variants
    return cleaned


def parse_llm_response(response_text, original_skills):
    canonical_to_variants = {}

    try:
        # Try to parse as JSON first
        parsed = json.loads(response_text.strip())

        # Extract variants from the new structure
        for canonical, data in parsed.items():
            if isinstance(data, dict) and 'variants' in data:
                canonical_to_variants[canonical] = data['variants']
            elif isinstance(data, list):
                # Fallback for old format
                canonical_to_variants[canonical] = data

    except Exception as e:
        print(f"JSON parsing failed: {e}, falling back to line parsing")
        # Fallback to line-by-line parsing
        lines = response_text.strip().split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue

            if ':' in line:
                parts = line.split(':', 1)
                if len(parts) == 2:
                    canonical = parts[0].strip()
                    variants_str = parts[1].strip()

                    if variants_str.startswith('[') and variants_str.endswith(']'):
                        try:
                            variants = json.loads(variants_str)
                            canonical_to_variants[canonical] = variants
                        except:
                            continue

    canonical_to_variants = sanitize_variants(canonical_to_variants, original_skills)

    # Ensure all original skills are covered - if not, add them as individual canonicals
    covered_skills = set()
    for variants in canonical_to_variants.values():
        covered_skills.update(variants)

    missing_skills = set(original_skills) - covered_skills
    if missing_skills:
        print(f"Adding {len(missing_skills)} missing skills as individual canonicals")
        for skill in missing_skills:
            canonical_to_variants[skill] = [skill]

    # Check if LLM actually grouped anything or just returned individuals
    grouped_count = sum(1 for variants in canonical_to_variants.values() if len(variants) > 1)
    total_groups = len(canonical_to_variants)
    print(f"LLM created {grouped_count}/{total_groups} actual groups (multi-variant)")

    return canonical_to_variants


def call_llm_for_clustering(skill_batch, existing_canonicals=None):
    skills_text = ', '.join(sorted(skill_batch))

    prompt = f"""Analyze these skills and create canonical groups. Be aggressive about grouping similar technologies!

### NORMALIZATION INSTRUCTIONS
- Collapse versions: "Java 8", "Java 11" → "Java"
- Group ALL variants of same technology together
- Prefer umbrella terms: "Spring Core", "Spring MVC" → "Spring Boot"
- Unify spelling/case: "Node.js", "nodejs" → "Node JS"
- Normalize punctuation: dots, hyphens, spaces
- Every variant must be an exact string from the provided skills list; never invent, rephrase, or shorten any variant
- Canonical names must reuse one of the provided variants or an identical string from the list; do not create new canonical labels
- Preserve the original casing and spacing of every provided skill in all variants
- Do not include placeholders like "N/A", "Unknown", or fabricated skills

### EXAMPLES
- ".NET", ".Net Framework", ".NET 3.0" → ".NET": [".NET", ".Net Framework", ".NET 3.0"]
- "Java 8", "Java 11", "Java SE" → "Java": ["Java 8", "Java 11", "Java SE"]
- "Spring", "Spring MVC", "Spring Boot" → "Spring Boot": ["Spring", "Spring MVC", "Spring Boot"]

### CONSTRAINTS
- Only merge CLEAR versions/spellings of same technology
- Keep different technologies separate: AWS EC2 ≠ AWS Lambda

### OUTPUT FORMAT
Return ONLY a JSON object where each canonical skill has reasoning:

{{
  "canonical_name": {{
    "variants": ["variant1", "variant2", "variant3"],
    "reasoning": "Why these variants were grouped together"
  }},
  "another_canonical": {{
    "variants": ["single_variant"],
    "reasoning": "This is a unique technology"
  }}
}}

Skills to analyze:
{skills_text}

{"Existing groups: " + str(existing_canonicals) if existing_canonicals else ""}

Return ONLY valid JSON:"""

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=AZURE_DEPLOYMENT,
                messages=[{"role": "user", "content": prompt}],
                # reasoning_effort="medium"
            )
            print("LLM call completed.")

            # Handle different response formats
            if hasattr(response, 'choices') and response.choices:
                response_text = response.choices[0].message.content
            else:
                print(f"Unexpected response format: {type(response)}")
                response_text = str(response)
            if response_text:
                print(f"LLM Response (first 200 chars): {response_text[:200]}...")
                result = parse_llm_response(response_text, skill_batch)
                if result:  # Only return if we got a valid result
                    return result

        except Exception as e:
            print(f"Error calling LLM (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                print("Retrying...")
                continue

    print(f"Failed to get valid response after {max_retries} attempts")
    return {}


def process_letter_batch(skill_batch):
    all_clustered_skills = set()
    canonical_groups = {}

    remaining_skills = set(skill_batch)
    iteration = 1

    while remaining_skills:
        print(f"Processing batch with {len(remaining_skills)} skills (iteration {iteration})")

        current_batch = list(remaining_skills)
        response_groups = call_llm_for_clustering(current_batch, canonical_groups if canonical_groups else None)

        if not response_groups:
            print("No response from LLM, keeping remaining skills as individual groups")
            for skill in remaining_skills:
                canonical_groups[skill] = [skill]
            break

        iteration_clustered = set()
        for canonical, variants in response_groups.items():
            if canonical not in canonical_groups:
                canonical_groups[canonical] = []
            canonical_groups[canonical].extend(variants)
            iteration_clustered.update(variants)

        all_clustered_skills.update(iteration_clustered)
        remaining_skills = remaining_skills - iteration_clustered

        if not remaining_skills:
            break

        iteration += 1
        if iteration > 5:  # Safety limit
            print("Max iterations reached, adding remaining skills individually")
            for skill in remaining_skills:
                canonical_groups[skill] = [skill]
            break

    return canonical_groups


def main():
    csv_file_path = 'data/DFC_YTD_2023-2025.csv'
    output_file_path = 'skills/skill_normalization_llm_52.json'

    # Initialize empty file
    with open(output_file_path, 'w', encoding='utf-8') as f:
        json.dump({}, f)

    print("Extracting all skills from CSV...")
    all_skills = extract_all_skills(csv_file_path)
    print(f"Found {len(all_skills)} unique skills")

    print("Grouping skills by first letter...")
    letter_groups = group_skills_by_first_letter(all_skills)

    all_canonical_groups = {}
    total_canonicals = 0

    for letter in sorted(letter_groups.keys(), key=lambda x: (x != '.', x)):
        skill_batch = letter_groups[letter]
        print(f"\nProcessing letter {letter}: {len(skill_batch)} skills")
        if len(skill_batch) <= 10:  # Show sample for small batches
            print(f"Sample skills: {sorted(skill_batch)[:10]}")

        # Process skills - handle large batches by processing each chunk independently
        # This avoids incorrect merging of canonicals across chunks
        batch_size = 100  # Process in chunks of 100 skills
        if len(skill_batch) > batch_size:
            print(f"Large batch detected ({len(skill_batch)} skills), splitting into independent chunks")

            letter_groups_result = {}
            for i in range(0, len(skill_batch), batch_size):
                chunk = skill_batch[i:i + batch_size]
                print(f"Processing chunk {i//batch_size + 1}/{(len(skill_batch) + batch_size - 1)//batch_size} ({len(chunk)} skills)")

                chunk_result = process_letter_batch(chunk)

                # Add chunk results without merging (each chunk is independent)
                for canonical, variants in chunk_result.items():
                    letter_groups_result[canonical] = variants
        else:
            letter_groups_result = process_letter_batch(skill_batch)

        for canonical, variants in letter_groups_result.items():
            if canonical in all_canonical_groups:
                # Merge variants when same canonical name appears in different batches
                existing_variants = all_canonical_groups[canonical]['variants']
                merged_variants = list(set(existing_variants + variants))
                all_canonical_groups[canonical] = {
                    'variants': merged_variants,
                    'variant_count': len(merged_variants)
                }
                print(f"Merged variants for '{canonical}': {len(existing_variants)} + {len(variants)} → {len(merged_variants)} total")
            else:
                all_canonical_groups[canonical] = {
                    'variants': list(set(variants)),  # Deduplicate
                    'variant_count': len(set(variants))
                }

        total_canonicals += len(letter_groups_result)

        # Save progress after each letter
        print(f"Letter {letter} completed. Saving progress...")
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(all_canonical_groups, f, indent=2, ensure_ascii=False)

        print(f"Progress saved. Current canonical groups: {len(all_canonical_groups)}")

    print(f"\nProcessed all letters. Total canonical groups: {total_canonicals}")

    # Final validation: Check that total variants equal total unique skills
    total_variants = sum(len(group['variants']) for group in all_canonical_groups.values())
    total_unique_skills = len(all_skills)
    print(f"Validation: {total_variants} total variants across all groups vs {total_unique_skills} unique skills extracted")

    if total_variants != total_unique_skills:
        print(f"WARNING: Variant count mismatch! Expected {total_unique_skills}, got {total_variants}")
    else:
        print("Validation passed: All skills accounted for in canonical groups")

    # Count occurrences of canonical skills
    canonical_counts = Counter()
    with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)

        for row in reader:
            if len(row) > 22:
                skills_cell = row[22]
                if not skills_cell or skills_cell.strip() == '':
                    continue

                skills = []
                comma_parts = skills_cell.split(',')
                for part in comma_parts:
                    semicolon_parts = part.split(';')
                    skills.extend(semicolon_parts)

                row_canonicals = set()
                for skill in skills:
                    skill = skill.strip()
                    if skill:
                        # Find which canonical group this skill belongs to
                        for canonical, data in all_canonical_groups.items():
                            if skill in data['variants']:
                                row_canonicals.add(canonical)
                                break

                for canonical in row_canonicals:
                    canonical_counts[canonical] += 1

    # Create final output
    final_output = {}
    for canonical, data in all_canonical_groups.items():
        final_output[canonical] = {
            'variants': data['variants'],
            'variant_count': data['variant_count'],
            'canonical_count': canonical_counts[canonical]
        }

    with open(output_file_path, 'w', encoding='utf-8') as f:
        json.dump(final_output, f, indent=2, ensure_ascii=False)

    print(f"Saved results to {output_file_path}")

    # Verify counts
    total_rows = sum(canonical_counts.values())
    print(f"Total row count verification: {total_rows}")


if __name__ == "__main__":
    main()
