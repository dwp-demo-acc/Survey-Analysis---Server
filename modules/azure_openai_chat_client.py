import os
from openai import AzureOpenAI, AsyncAzureOpenAI

global_mode = os.getenv("LLM_MODE")
print('\n\n\n---------------global_mode: ', global_mode)

# docs :
# source github:  https://github.com/openai/openai-python
# source microsoft : https://learn.microsoft.com/en-us/azure/ai-services/openai/chatgpt-quickstart?tabs=bash%2Cpython-new&pivots=programming-language-python

class AzOpenAIClient():
    _api_key = os.getenv('OPENAI_API_KEY')
    _api_version = "2023-07-01-preview"
    _api_base = "https://ddaigpt4.openai.azure.com/"

    def connect(self):
        return AzureOpenAI(
            azure_endpoint=self._api_base,
            api_key=self._api_key,
            api_version=self._api_version
        )

    async def connectAsync(self):
        return await AsyncAzureOpenAI(
            azure_endpoint=self._api_base,
            api_key=self._api_key,
            api_version=self._api_version
        )


class ChatModel():
    _default_config = {
        "model": "ddai_gpt4_32k",
    }

    def __init__(self, type="sync"):
        if type == "async":
            self.client = AzOpenAIClient().connectAsync()
        else:
            self.client = AzOpenAIClient().connect()

    def create_prompt(self, system_content="", user_content=""):
        system_message = {"role": "system", "content": system_content} if system_content else None
        user_message = {"role": "user", "content": user_content}
        return [message for message in [system_message, user_message] if message]

    def call_model(self, prompt, config={}):
        if global_mode == 'dev':
            return "Module On Dev Mode : Skip On LLM "
        try:
            merged_config = {**self._default_config, **config}

            if type(prompt) == str:
                prompt = self.create_prompt(None, prompt)

            response = self.client.chat.completions.create(
                **merged_config,
                messages=prompt

            )

            return response.choices[0].message.content
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def __str__(self):
        model = self._default_config["model"]
        return f"OpenAI Chat Model : {model}"
