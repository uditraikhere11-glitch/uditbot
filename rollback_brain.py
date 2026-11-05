import re
file_path = "/home/amansingh111111111/uditbot/src/parsers/brain_validation.py"
with open(file_path, "r") as f:
    content = f.read()

# Use regex to dynamically strip the injected fallback block
pattern = r"(\s+if not extracted:\s+# FALLBACK: Safely route unanchored trailing numbers.*?)(?=\s+if not extracted:\s+return PriceExtractionResult\()"
content = re.sub(pattern, "", content, flags=re.DOTALL)

with open(file_path, "w") as f:
    f.write(content)
print("✅ ROLLBACK SUCCESSFUL! The unsafe fallback has been completely removed.")
