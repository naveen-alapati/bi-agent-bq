from typing import List, Dict, Any, Optional
import os
import json


class LLMClient:
    def __init__(self) -> None:
        self.provider = os.getenv("LLM_PROVIDER", "vertex")  # vertex | openai
        self.vertex_location = os.getenv("VERTEX_LOCATION", os.getenv("BQ_LOCATION", "us-central1"))
        self.vertex_model = os.getenv("VERTEX_LLM_MODEL", "gemini-1.5-pro-001")
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.openai_model = os.getenv("OPENAI_LLM_MODEL", "gpt-4o-mini")
        self.project_id = os.getenv("PROJECT_ID")

    def generate_json(self, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        if self.provider == "vertex":
            return self._generate_vertex(system_prompt, user_prompt)
        if self.provider == "openai":
            return self._generate_openai(system_prompt, user_prompt)
        raise RuntimeError("Unsupported LLM_PROVIDER")

    def _generate_vertex(self, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        from google.cloud import aiplatform
        from vertexai.preview.generative_models import GenerativeModel

        aiplatform.init(project=self.project_id, location=self.vertex_location)
        model = GenerativeModel(self.vertex_model)
        prompt = f"SYSTEM: {system_prompt}\n\nINPUT_DATA: {user_prompt}"
        result = model.generate_content(prompt)
        text = result.candidates[0].content.parts[0].text
        return json.loads(text)

    def _generate_openai(self, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        from openai import OpenAI
        client = OpenAI(api_key=self.openai_api_key)
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        resp = client.chat.completions.create(model=self.openai_model, messages=messages, response_format={"type": "json_object"})
        text = resp.choices[0].message.content
        return json.loads(text)

    def diagnostics(self) -> Dict[str, Any]:
        try:
            if self.provider == "vertex":
                from google.cloud import aiplatform
                from vertexai.preview.generative_models import GenerativeModel
                aiplatform.init(project=self.project_id, location=self.vertex_location)
                model = GenerativeModel(self.vertex_model)
                result = model.generate_content("SYSTEM: respond with {\"ok\":true}")
                text = result.candidates[0].content.parts[0].text
                return {"provider": "vertex", "ok": True, "raw": text}
            elif self.provider == "openai":
                from openai import OpenAI
                client = OpenAI(api_key=self.openai_api_key)
                resp = client.chat.completions.create(model=self.openai_model, messages=[{"role": "user", "content": "{\"ok\":true}"}], response_format={"type":"json_object"})
                return {"provider": "openai", "ok": True, "raw": resp.choices[0].message.content}
            else:
                return {"provider": self.provider, "ok": False, "error": "unsupported provider"}
        except Exception as exc:
            return {"provider": self.provider, "ok": False, "error": str(exc)}