import re

file_path = "/home/amansingh111111111/uditbot/src/parsers/brain_validation.py"
with open(file_path, "r") as f:
    content = f.read()

target_block = """    if not extracted:
        return PriceExtractionResult(
            status=PriceStatus.ABSENT,
            entry_min=0.0,
            entry_max=0.0,
            source_span="",
            span_offsets=None,
            reason_code=DecisionReasonCode.NO_ENTRY_PREMIUM.value,
            reason="Missing mandatory entry premium for option signal (SL/Target is not an entry price)",
        )"""

fallback_code = """    if not extracted:
        # FALLBACK: Catch raw numbers immediately following the CE/PE token (e.g. " 90-100")
        fallback_m = re.match(r"^\\s*[\\:\\-\\@]*\\s*(\\d+(?:\\.\\d+)?)(?:\\s*(?:[-~_]+|to|and|/|&|\\||,)\\s*(\\d+(?:\\.\\d+)?))?", masked_sub, re.IGNORECASE)
        if fallback_m:
            val1 = float(fallback_m.group(1))
            val2 = float(fallback_m.group(2)) if fallback_m.group(2) else val1
            upper_limit = max(val1, val2)
            span = fallback_m.group(0).strip()
            abs_start = base_offset + fallback_m.start()
            abs_end = base_offset + fallback_m.end()
            extracted.append((upper_limit, upper_limit, span, (abs_start, abs_end)))

    if not extracted:
        return PriceExtractionResult(
            status=PriceStatus.ABSENT,
            entry_min=0.0,
            entry_max=0.0,
            source_span="",
            span_offsets=None,
            reason_code=DecisionReasonCode.NO_ENTRY_PREMIUM.value,
            reason="Missing mandatory entry premium for option signal (SL/Target is not an entry price)",
        )"""

if target_block in content:
    content = content.replace(target_block, fallback_code)
    with open(file_path, "w") as f:
        f.write(content)
    print("✅ BRAIN PATCHED SUCCESSFULLY! The Upper Limit Fallback is now active.")
else:
    print("⚠️ Could not find the target block. It may already be patched.")
