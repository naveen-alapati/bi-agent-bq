from typing import List, Dict, Any, Optional
import os
import json
import requests


class LLMClient:
    def __init__(self) -> None:
        self.provider = os.getenv("LLM_PROVIDER", "vertex")  # vertex | openai | gemini
        self.vertex_location = os.getenv("VERTEX_LOCATION", os.getenv("BQ_LOCATION", "us-central1"))
        self.vertex_model = os.getenv("VERTEX_LLM_MODEL", "gemini-1.5-pro-001")
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.openai_model = os.getenv("OPENAI_LLM_MODEL", "gpt-4o-mini")
        self.project_id = os.getenv("PROJECT_ID")
        # Gemini REST
        self.gemini_api_key = os.getenv("GEMINI_API_KEY")
        self.gemini_model = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

    def generate_json(self, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        try:
            print(f"LLM generate_json called with provider: {self.provider}")
            print(f"System prompt length: {len(system_prompt)}")
            print(f"User prompt length: {len(user_prompt)}")
            
            if self.provider == "vertex":
                return self._generate_vertex(system_prompt, user_prompt)
            if self.provider == "openai":
                return self._generate_openai(system_prompt, user_prompt)
            if self.provider == "gemini":
                if not self.gemini_api_key:
                    print(f"GEMINI_API_KEY not set. Available env vars: {[k for k in os.environ.keys() if 'GEMINI' in k or 'LLM' in k]}")
                    raise RuntimeError("GEMINI_API_KEY must be set when LLM_PROVIDER=gemini")
                return self._generate_gemini(system_prompt, user_prompt)
            raise RuntimeError(f"Unsupported LLM_PROVIDER: {self.provider}")
        except Exception as exc:
            print(f"Error in generate_json: {exc}")
            import traceback
            traceback.print_exc()
            raise

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

    def _parse_json_text(self, text: str) -> Dict[str, Any]:
        s = text.strip()
        # strip code fences if present
        if s.startswith("```"):
            s = s.strip('`')
            if s.startswith("json"):
                s = s[4:].strip()
        return json.loads(s)

    def _generate_gemini(self, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        if not self.gemini_api_key:
            raise RuntimeError("GEMINI_API_KEY must be set when LLM_PROVIDER=gemini")
        
        print(f"Calling Gemini API with model: {self.gemini_model}")
        print(f"API key length: {len(self.gemini_api_key) if self.gemini_api_key else 0}")
        
        endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{self.gemini_model}:generateContent"
        headers = {"Content-Type": "application/json"}
        body = {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {"text": f"SYSTEM: {system_prompt}\n\nINPUT_DATA: {user_prompt}"}
                    ],
                }
            ],
            "generationConfig": {
                "responseMimeType": "application/json"
            }
        }
        
        print(f"Making request to: {endpoint}")
        print(f"Request body size: {len(json.dumps(body))}")
        
        try:
            resp = requests.post(f"{endpoint}?key={self.gemini_api_key}", headers=headers, data=json.dumps(body), timeout=20)
            print(f"Gemini API response status: {resp.status_code}")
            print(f"Gemini API response headers: {dict(resp.headers)}")
            
            if resp.status_code != 200:
                print(f"Gemini API error response: {resp.text}")
                raise RuntimeError(f"Gemini API error {resp.status_code}: {resp.text}")
            
            data = resp.json()
            print(f"Gemini API response data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            
            try:
                text = data["candidates"][0]["content"]["parts"][0]["text"]
                print(f"Extracted text from response: {text[:100]}...")
                return self._parse_json_text(text)
            except Exception as exc:
                print(f"Failed to parse Gemini response structure: {data}")
                raise RuntimeError(f"Failed to parse Gemini response: {data}") from exc
                
        except requests.exceptions.RequestException as req_exc:
            print(f"Request exception: {req_exc}")
            raise RuntimeError(f"Gemini API request failed: {req_exc}") from req_exc
        except Exception as exc:
            print(f"Unexpected error in Gemini API call: {exc}")
            raise RuntimeError(f"Gemini API call failed: {exc}") from exc

    def edit_sql(self, original_sql: str, instruction: str) -> str:
        system = (
            "You are a SQL assistant. Output only JSON with a single key 'sql'. "
            "Rewrite the provided BigQuery SQL according to the user's instruction. "
            "Keep the same output columns and aliases (x,y or label,value) unless specifically asked to change. "
            "Ensure BigQuery Standard SQL compatibility and add safe NULL handling as needed."
        )
        user = json.dumps({"sql": original_sql, "instruction": instruction})
        if self.provider == "openai":
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_api_key)
            resp = client.chat.completions.create(model=self.openai_model, messages=[{"role":"system","content":system},{"role":"user","content":user}], response_format={"type":"json_object"})
            return json.loads(resp.choices[0].message.content)["sql"]
        if self.provider == "vertex":
            from google.cloud import aiplatform
            from vertexai.preview.generative_models import GenerativeModel
            aiplatform.init(project=self.project_id, location=self.vertex_location)
            model = GenerativeModel(self.vertex_model)
            out = model.generate_content(f"SYSTEM: {system}\n\nINPUT_DATA: {user}")
            return json.loads(out.candidates[0].content.parts[0].text)["sql"]
        if self.provider == "gemini":
            endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{self.gemini_model}:generateContent"
            headers = {"Content-Type": "application/json"}
            body = {
                "contents": [{"role": "user", "parts": [{"text": f"SYSTEM: {system}\n\nINPUT_DATA: {user}"}]}],
                "generationConfig": {"responseMimeType": "application/json"}
            }
            resp = requests.post(f"{endpoint}?key={self.gemini_api_key}", headers=headers, data=json.dumps(body), timeout=20)
            data = resp.json()
            text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "{}")
            return json.loads(text).get("sql", original_sql)
        raise RuntimeError("Unsupported provider for edit_sql")

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
            elif self.provider == "gemini":
                endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{self.gemini_model}:generateContent"
                headers = {"Content-Type": "application/json"}
                body = {
                    "contents": [{"role": "user", "parts": [{"text": "{\"ok\":true}"}]}],
                    "generationConfig": {"responseMimeType": "application/json"}
                }
                resp = requests.post(f"{endpoint}?key={self.gemini_api_key}", headers=headers, data=json.dumps(body), timeout=10)
                if resp.status_code != 200:
                    return {"provider": "gemini", "ok": False, "status": resp.status_code, "error": resp.text}
                return {"provider": "gemini", "ok": True, "raw": resp.text}
            else:
                return {"provider": self.provider, "ok": False, "error": "unsupported provider"}
        except Exception as exc:
            return {"provider": self.provider, "ok": False, "error": str(exc)}