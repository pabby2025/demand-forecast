# --------------------------- LLM ENDPOINT --------------------------- #

from openai import OpenAI

OPENAI_ENDPOINT = "https://openai-demandforcast-np.openai.azure.com/openai/v1"
DEPLOYMENT_NAME = "gpt-5-mini"
OPENAI_KEY = "2xdHQYttik9ibhwKJJnd6VZ1XKDh82X562p3TwFtHl8GZoJ1DsQzJQQJ99BKAC77bzfXJ3w3AAABACOG1wzL"

client = OpenAI(base_url=OPENAI_ENDPOINT, api_key=OPENAI_KEY)

completion = client.chat.completions.create(
    model=DEPLOYMENT_NAME,
    messages=[
        {
            "role": "user",
            "content": "What is the capital of France?",
        }
    ],
)
print(completion)

# --------------------------- TEXT EMBEDDING MODEL ENDPOINT --------------------------- #

from openai import AzureOpenAI

AZURE_OPENAI_ENDPOINT = "https://openai-demandforcast-np.openai.azure.com/"
AZURE_DEPLOYMENT_NAME = "text-embedding-3-large"
API_VERSION = "2024-12-01-preview"
AZURE_OPENAI_KEY = "2xdHQYttik9ibhwKJJnd6VZ1XKDh82X562p3TwFtHl8GZoJ1DsQzJQQJ99BKAC77bzfXJ3w3AAABACOG1wzL"

client = AzureOpenAI(
    api_version=API_VERSION,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_KEY,
)

response = client.embeddings.create(
    input=["first phrase", "second phrase", "third phrase"], model=AZURE_DEPLOYMENT_NAME
)
for item in response.data:
    length = len(item.embedding)
    print(
        f"data[{item.index}]: length={length}, "
        f"[{item.embedding[0]}, {item.embedding[1]}, "
        f"..., {item.embedding[length-2]}, {item.embedding[length-1]}]"
    )
print(response.usage)
