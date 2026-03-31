import os
"""
Skill Normalization LLM - Phase 2 (Higher-Level Consolidation)

This script takes the canonical groups from the first normalization phase
and performs higher-level grouping to create consolidated skill categories.

Process:
1. Load canonical groups from phase 1 (skills_normalization_llm.json)
2. Identify any missing skills not covered in phase 1 and add them as single variants
3. Process all canonical names with LLM to create higher-level consolidated categories
4. Merge variants back under the new consolidated group names
5. Ensure all 3490 original skills are accounted for in the final output
6. Save to skills_normalization_llm_final.json sorted alphabetically

Example: "Java", "Python", "JavaScript" → "Programming Languages"
"""

import csv
import json
import re
from collections import defaultdict, Counter
from openai import OpenAI

AZURE_OPENAI_KEY = os.environ.get("AZURE_OPENAI_KEY", "")
AZURE_OPENAI_ENDPOINT = "https://openai-demandforcast-np.openai.azure.com/openai/v1"
AZURE_DEPLOYMENT = "gpt-5-mini"

client = OpenAI(
    base_url=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_KEY,
)


def extract_all_skills(csv_file_path, skills_column_index=22):
    """Extract all unique skills from CSV file."""
    all_skills = set()

    with open(csv_file_path, "r", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # Skip header

        for row in reader:
            if len(row) > skills_column_index:
                skills_cell = row[skills_column_index]
                if not skills_cell or skills_cell.strip() == "":
                    continue

                skills = []
                comma_parts = skills_cell.split(",")
                for part in comma_parts:
                    semicolon_parts = part.split(";")
                    skills.extend(semicolon_parts)

                for skill in skills:
                    skill = skill.strip()
                    if skill:
                        all_skills.add(skill)

    return all_skills


def load_existing_normalization(input_file_path):
    """Load the canonical groups from the first normalization phase."""
    with open(input_file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_canonical_names(canonical_groups):
    """Extract all canonical group names (keys) from the normalization data."""
    return list(canonical_groups.keys())


def identify_missing_skills(all_skills, canonical_groups):
    """Find skills that weren't included in any canonical group."""
    covered_skills = set()
    for canonical_data in canonical_groups.values():
        covered_skills.update(canonical_data["variants"])

    missing_skills = all_skills - covered_skills
    print(
        f"Found {len(missing_skills)} skills not covered by existing canonical groups"
    )

    return missing_skills


def create_single_variant_groups(missing_skills):
    """Create single-variant canonical groups for missing skills."""
    single_groups = {}
    for skill in missing_skills:
        single_groups[skill] = {
            "variants": [skill],
            "variant_count": 1,
            "canonical_count": 0,  # Will be calculated later
        }
    return single_groups


def group_canonicals_by_first_letter(canonical_names):
    """Group canonical names by their first letter for batch processing."""
    groups = defaultdict(list)
    for canonical in canonical_names:
        first_char = canonical[0].upper()
        if first_char.isalpha():
            groups[first_char].append(canonical)
        elif first_char == ".":
            groups["."].append(canonical)
    return groups


def parse_llm_response_phase2(response_text, original_canonicals):
    """Parse LLM response for higher-level grouping of canonical names."""
    group_to_canonicals = {}

    try:
        parsed = json.loads(response_text.strip())

        for group_name, data in parsed.items():
            if isinstance(data, dict) and "canonicals" in data:
                canonicals = data["canonicals"]
                if isinstance(canonicals, list):
                    group_to_canonicals[group_name] = canonicals
                else:
                    print(
                        f"Warning: 'canonicals' field is not a list for group '{group_name}': {canonicals}"
                    )
                    group_to_canonicals[group_name] = [str(canonicals)]
            elif isinstance(data, list):
                group_to_canonicals[group_name] = data
            else:
                print(
                    f"Warning: Unexpected data format for group '{group_name}': {type(data)}"
                )

    except Exception as e:
        print(f"JSON parsing failed: {e}")
        print(f"Response text (first 500 chars): {response_text[:500]}")
        # Try to extract JSON from the response if it's wrapped in text
        import re

        json_match = re.search(r"\{.*\}", response_text, re.DOTALL)
        if json_match:
            try:
                parsed = json.loads(json_match.group())
                for group_name, data in parsed.items():
                    if isinstance(data, dict) and "canonicals" in data:
                        canonicals = data["canonicals"]
                        if isinstance(canonicals, list):
                            group_to_canonicals[group_name] = canonicals
                        else:
                            group_to_canonicals[group_name] = [str(canonicals)]
                    elif isinstance(data, list):
                        group_to_canonicals[group_name] = data
            except Exception as e2:
                print(f"Extracted JSON parsing also failed: {e2}")
                return {}

    # If we still have no groups, return empty and let caller handle fallback
    if not group_to_canonicals:
        print("No valid groups parsed from LLM response")
        return {}

    # Ensure all original canonicals are covered
    covered_canonicals = set()
    for canonicals in group_to_canonicals.values():
        if isinstance(canonicals, list):
            covered_canonicals.update(canonicals)

    missing_canonicals = set(original_canonicals) - covered_canonicals
    if missing_canonicals:
        print(
            f"Adding {len(missing_canonicals)} missing canonicals as individual groups"
        )
        for canonical in missing_canonicals:
            group_to_canonicals[canonical] = [canonical]

    grouped_count = sum(
        1
        for canonicals in group_to_canonicals.values()
        if isinstance(canonicals, list) and len(canonicals) > 1
    )
    total_groups = len(group_to_canonicals)
    print(
        f"LLM created {grouped_count}/{total_groups} actual merged groups (multi-canonical)"
    )

    return group_to_canonicals


def call_llm_for_higher_level_grouping(canonical_batch):
    """
    Call LLM to perform higher-level grouping of canonical names with improved prompting.
    """
    canonicals_text = ", ".join(sorted(canonical_batch))

    prompt = f"""Analyze these canonical skill groups and create higher-level consolidated categories.
Be aggressive about creating umbrella categories for related technologies! Use step-by-step reasoning to ensure accurate grouping.

### CONSOLIDATION INSTRUCTIONS
- Group related technologies under umbrella terms
- Collapse framework families: "Spring Boot", "Spring MVC", "Spring Core" → "Spring Framework"
- Unify cloud platforms: "AWS EC2", "AWS Lambda", "AWS S3" → "Amazon Web Services (AWS)"
- Merge database types: "MySQL", "PostgreSQL", "MongoDB" → "Databases"
- Combine programming languages: "Java", "Python", "JavaScript" → "Programming Languages"
- Group development tools: "Git", "Jenkins", "Docker" → "DevOps Tools"

### EXAMPLES
- "Java", "Python", "JavaScript", "C#" → "Programming Languages": ["Java", "Python", "JavaScript", "C#"]
- "AWS EC2", "AWS Lambda", "AWS S3", "Azure VMs" → "Cloud Computing": ["AWS EC2", "AWS Lambda", "AWS S3", "Azure VMs"]
- "MySQL", "PostgreSQL", "MongoDB", "Redis" → "Databases": ["MySQL", "PostgreSQL", "MongoDB", "Redis"]

### CHAIN OF THOUGHT REASONING
1. Identify commonalities between technologies (e.g., same family, purpose, or platform).
2. Justify why these technologies belong together under a single category.
3. Ensure no unrelated technologies are grouped together.
4. Provide reasoning for each group to explain the decision.

### OUTPUT FORMAT
Return ONLY a JSON object where each consolidated category contains the canonical groups:

{{
  "Consolidated Category Name": {{
    "canonicals": ["canonical1", "canonical2", "canonical3"],
    "reasoning": "Why these canonicals were grouped together"
  }},
  "Another Category": {{
    "canonicals": ["single_canonical"],
    "reasoning": "This is a unique technology category"
  }}
}}

Canonical groups to consolidate:
{canonicals_text}

Return ONLY valid JSON:"""

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=AZURE_DEPLOYMENT,
                messages=[{"role": "user", "content": prompt}],
                reasoning_effort="medium",
            )
            print("LLM call completed.")

            # Handle different response formats
            if hasattr(response, "choices") and response.choices:
                response_text = response.choices[0].message.content
            else:
                print(f"Unexpected response format: {type(response)}")
                response_text = str(response)
            if response_text:
                print(f"LLM Response (first 200 chars): {response_text[:200]}...")
                result = parse_llm_response_phase2(response_text, canonical_batch)
                if result:  # Only return if we got a valid result
                    return result

        except Exception as e:
            print(f"Error calling LLM (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                print("Retrying...")
                continue

    print(f"Failed to get valid response after {max_retries} attempts")
    return {}


def process_canonical_batch(canonical_batch):
    """Process a batch of canonical names through LLM for higher-level grouping."""
    print(f"Processing batch with {len(canonical_batch)} canonicals")

    response_groups = call_llm_for_higher_level_grouping(canonical_batch)

    if not response_groups:
        print("No response from LLM, keeping canonicals as individual groups")
        return {canonical: [canonical] for canonical in canonical_batch}

    return response_groups


def merge_canonical_groups(
    group_to_canonicals, original_canonical_groups, single_variant_groups
):
    """Merge the higher-level groups back into the original variant lists."""
    final_groups = {}

    # If no groups were created (LLM failed), return original groups
    if not group_to_canonicals:
        print("No merged groups to process, returning original canonical groups")
        return {**original_canonical_groups, **single_variant_groups}

    for group_name, canonical_list in group_to_canonicals.items():
        merged_variants = []
        total_count = 0

        for canonical in canonical_list:
            if canonical in original_canonical_groups:
                merged_variants.extend(original_canonical_groups[canonical]["variants"])
                total_count += original_canonical_groups[canonical]["canonical_count"]
            elif canonical in single_variant_groups:
                merged_variants.extend(single_variant_groups[canonical]["variants"])
                total_count += single_variant_groups[canonical]["canonical_count"]
            else:
                print(f"Warning: Canonical '{canonical}' not found in either group")

        final_groups[group_name] = {
            "variants": list(set(merged_variants)),  # Remove duplicates
            "variant_count": len(set(merged_variants)),
            "canonical_count": total_count,
        }

    return final_groups


def main():
    csv_file_path = "input/DFC_YTD_2023-2025.csv"
    input_file_path = "input/step1_skill_normalization_llm.json"
    output_file_path = "input/skill_normalization_llm_final.json"

    print("Loading existing normalization results...")
    canonical_groups = load_existing_normalization(input_file_path)
    print(f"Loaded {len(canonical_groups)} canonical groups")

    print("Extracting all skills from CSV...")
    all_skills = extract_all_skills(csv_file_path)
    print(f"Found {len(all_skills)} unique skills")

    # Identify and handle missing skills
    missing_skills = identify_missing_skills(all_skills, canonical_groups)
    print(f"Identified {len(missing_skills)} missing skills that need to be added")
    single_variant_groups = create_single_variant_groups(missing_skills)

    # Add single variants to canonical groups for processing
    all_canonical_groups = {**canonical_groups, **single_variant_groups}

    print(
        f"Total canonical groups after adding missing skills: {len(all_canonical_groups)}"
    )
    total_variants_before = sum(
        len(group["variants"]) for group in all_canonical_groups.values()
    )
    print(f"Total variants before higher-level grouping: {total_variants_before}")

    # Get all canonical names for higher-level grouping
    canonical_names = get_canonical_names(all_canonical_groups)
    print(
        f"Processing {len(canonical_names)} canonical names for higher-level grouping"
    )

    print("Grouping canonicals by first letter...")
    letter_groups = group_canonicals_by_first_letter(canonical_names)

    all_merged_groups = {}
    total_processed = 0

    for letter in sorted(letter_groups.keys(), key=lambda x: (x != ".", x)):
        canonical_batch = letter_groups[letter]
        print(f"\nProcessing letter {letter}: {len(canonical_batch)} canonicals")

        # Process in chunks if batch is large
        batch_size = 200  # Smaller batches for higher-level grouping
        if len(canonical_batch) > batch_size:
            print(
                f"Large batch detected ({len(canonical_batch)} canonicals), splitting into chunks"
            )

            letter_results = {}
            for i in range(0, len(canonical_batch), batch_size):
                chunk = canonical_batch[i : i + batch_size]
                print(
                    f"Processing chunk {i//batch_size + 1}/{(len(canonical_batch) + batch_size - 1)//batch_size} ({len(chunk)} canonicals)"
                )

                chunk_result = process_canonical_batch(chunk)

                # Merge chunk results
                for group_name, canonicals in chunk_result.items():
                    if group_name in letter_results:
                        letter_results[group_name].extend(canonicals)
                    else:
                        letter_results[group_name] = canonicals
        else:
            letter_results = process_canonical_batch(canonical_batch)

        # Merge letter results into final groups
        for group_name, canonicals in letter_results.items():
            if group_name in all_merged_groups:
                existing_canonicals = all_merged_groups[group_name]["canonicals"]
                merged_canonicals = list(set(existing_canonicals + canonicals))
                all_merged_groups[group_name]["canonicals"] = merged_canonicals
                print(
                    f"Merged canonicals for '{group_name}': {len(existing_canonicals)} + {len(canonicals)} → {len(merged_canonicals)} total"
                )
            else:
                all_merged_groups[group_name] = {"canonicals": canonicals}

        total_processed += len(letter_results)
        print(
            f"Letter {letter} completed. Current merged groups: {len(all_merged_groups)}"
        )

    print(f"\nProcessed all letters. Total merged groups: {total_processed}")

    # Now merge back to get final variant lists
    print("Merging back to variant lists...")
    final_groups = merge_canonical_groups(
        all_merged_groups, canonical_groups, single_variant_groups
    )

    # If merging failed (empty variants), fall back to original canonical groups
    if final_groups and all(
        len(group["variants"]) == 0 for group in final_groups.values()
    ):
        print("Merging failed, falling back to original canonical groups...")
        final_groups = all_canonical_groups

    # Sort final groups alphabetically
    final_groups = dict(sorted(final_groups.items()))

    # Final validation
    total_variants = sum(len(group["variants"]) for group in final_groups.values())
    total_unique_skills = len(all_skills)
    print(
        f"Validation: {total_variants} total variants across all groups vs {total_unique_skills} unique skills extracted"
    )

    if total_variants != total_unique_skills:
        print(
            f"❌ CRITICAL: Variant count mismatch! Expected {total_unique_skills}, got {total_variants}"
        )
        print("This means some skills are missing from the final output!")
        print("The missing skills will be added as individual groups...")

        # Identify which skills are missing and add them
        covered_skills = set()
        for group in final_groups.values():
            covered_skills.update(group["variants"])

        missing_from_final = all_skills - covered_skills
        if missing_from_final:
            print(
                f"Adding {len(missing_from_final)} missing skills as individual canonical groups"
            )
            for skill in missing_from_final:
                final_groups[skill] = {
                    "variants": [skill],
                    "variant_count": 1,
                    "canonical_count": 0,  # Will be calculated later
                }

        # Recalculate after adding missing skills
        total_variants = sum(len(group["variants"]) for group in final_groups.values())
        print(f"After adding missing skills: {total_variants} total variants")

        # Sort and save after fixing missing skills
        final_groups = dict(sorted(final_groups.items()))
        with open(output_file_path, "w", encoding="utf-8") as f:
            json.dump(final_groups, f, indent=2, ensure_ascii=False)
        print(f"Updated final results saved to {output_file_path}")

    else:
        print("✓ Validation passed: All skills accounted for in final groups")

        # Sort final groups alphabetically
        final_groups = dict(sorted(final_groups.items()))

        # Save final results
        with open(output_file_path, "w", encoding="utf-8") as f:
            json.dump(final_groups, f, indent=2, ensure_ascii=False)

    print(f"Saved final consolidated results to {output_file_path}")

    # Final statistics (after all processing is complete)
    total_final_groups = len(final_groups)
    total_final_variants = sum(
        len(group["variants"]) for group in final_groups.values()
    )
    total_rows = sum(group["canonical_count"] for group in final_groups.values())

    print(f"\n=== FINAL STATISTICS ===")
    print(f"Total consolidated groups: {total_final_groups}")
    print(f"Total variants across all groups: {total_final_variants}")
    print(f"Total row count verification: {total_rows}")

    # Ensure we have all 3490 skills
    if total_final_variants == 3490:
        print("✅ SUCCESS: All 3490 skills accounted for!")
    else:
        print(
            f"❌ CRITICAL ERROR: Expected 3490 skills, got {total_final_variants} ({3490 - total_final_variants} still missing)"
        )

    # Ensure we have all 3490 skills
    if total_final_variants == 3490:
        print("✅ SUCCESS: All 3490 skills accounted for!")
    else:
        print(
            f"❌ WARNING: Expected 3490 skills, got {total_final_variants} ({3490 - total_final_variants} missing)"
        )


if __name__ == "__main__":
    main()
