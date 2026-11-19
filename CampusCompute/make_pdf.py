#!/usr/bin/env python3
"""Combine S01.png..S24.png and captions.txt into <SRN>.pdf (one screenshot + one-line caption per page)."""
import os
import sys

from PIL import Image, ImageDraw, ImageFont

if len(sys.argv) != 2:
    sys.exit("usage: python3 make_pdf.py YOUR_FULL_SRN   (run in the folder holding S01.png ... and captions.txt)")

srn = sys.argv[1]
captions = {}
with open("captions.txt", encoding="utf-8") as fh:
    for line in fh:
        if "|" in line:
            sid, text = line.rstrip("\n").split("|", 1)
            captions[sid] = text

try:
    font = ImageFont.truetype("DejaVuSans.ttf", 24)
except OSError:
    font = ImageFont.load_default()

PAGE_W, MARGIN = 1240, 40
pages, missing = [], []
for sid in sorted(captions):
    path = next((f"{sid}.{e}" for e in ("png", "jpg", "jpeg") if os.path.exists(f"{sid}.{e}")), None)
    if path is None:
        missing.append(sid)
        continue
    img = Image.open(path).convert("RGB")
    w = PAGE_W - 2 * MARGIN
    img = img.resize((w, max(1, int(img.height * w / img.width))))
    page = Image.new("RGB", (PAGE_W, img.height + 170), "white")
    draw = ImageDraw.Draw(page)
    draw.text((MARGIN, 20), sid, fill="black", font=font)
    page.paste(img, (MARGIN, 70))
    draw.text((MARGIN, img.height + 90), captions[sid], fill="black", font=font)
    pages.append(page)

if missing:
    print("WARNING - no image found for:", ", ".join(missing))
if not pages:
    sys.exit("no screenshots found")
pages[0].save(f"{srn}.pdf", save_all=True, append_images=pages[1:])
print(f"wrote {srn}.pdf with {len(pages)} page(s)")
