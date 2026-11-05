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

safe_fallback_code = """    if not extracted:
        # FALLBACK: Safely route unanchored trailing numbers through the native stream parser
        clean_tail = masked_sub.lstrip(" :-_")
        
        # Protect against grabbing an expiry date (e.g., '28 OCT') by ensuring no month token immediately follows
        if clean_tail and not re.match(r"^\\d{1,2}\\s*(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\\b", clean_tail, re.IGNORECASE):
            # Pass an empty anchor string to reuse the native parser's strict safety checks!
            status, lo, hi, span, r_code, r_msg = _parse_and_validate_price_token_stream("", clean_tail)
            
            if status == PriceStatus.VALID:
                # Apply the strict Upper Limit Rule requested for Ram DHS
                upper_limit = max(lo, hi)
                
                # Calculate safe offsets
                match_start = base_offset + (len(masked_sub) - len(clean_tail))
                match_end = match_start + len(span)
                
                extracted.append((upper_limit, upper_limit, span.strip(), (match_start, match_end)))

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
    content = content.replace(target_block, safe_fallback_code)
    with open(file_path, "w") as f:
        f.write(content)
    print("✅ SAFE BRAIN PATCHED SUCCESSFULLY! The native parser is now handling unanchored premiums.")
else:
    print("⚠️ Could not find the target block. It may have already been modified.")
