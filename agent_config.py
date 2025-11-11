# ai_chat_fix.py
from dotenv import load_dotenv
import os
import google.generativeai as genai

load_dotenv()
api_key = os.getenv("GOOGLE_API_KEY")
if not api_key:
    raise SystemExit("❌ GOOGLE_API_KEY not found in .env (make sure .env is in the same folder).")

genai.configure(api_key=api_key)

print(">>> Listing models available for this key (first 50):\n")
try:
    models = list(genai.list_models())  # make it a list so we can print length + sample
    if not models:
        print("No models returned. This usually means the key/project does not have Generative API access.")
    else:
        for i, m in enumerate(models[:50], start=1):
            # model attribute name differs by SDK, print whole object too if attribute missing
            model_id = getattr(m, "model", None) or getattr(m, "name", None) or str(m)
            print(f"{i:02d}. {model_id}")
except Exception as e:
    print("Error while listing models:", repr(e))
    raise SystemExit("Stop - fix list_models error first.")

# If you see a model like 'gemini-2.5-flash' or 'gemini-2.5-pro' below, set it to MODEL.
# Replace with one from your printed list.
MODEL = None  # ← after running once, put a supported model string here, e.g. "gemini-2.5-flash"

if MODEL:
    try:
        print(f"\n>>> Trying model {MODEL} to generate a short response:")
        model = genai.GenerativeModel(MODEL)
        resp = model.generate_content("Hello from test script — how are you?")
        # Some SDK responses may contain text in different attributes — check them
        text = getattr(resp, "text", None) or getattr(resp, "output", None) or str(resp)
        print("Response:", text)
    except Exception as e:
        print("Error calling generate_content:", repr(e))
else:
    print("\nNo MODEL selected. Re-run after choosing one of the printed model IDs and set MODEL variable.")
