from PIL import Image, ImageDraw, ImageFont
def icon(size, maskable=False):
    S=size*4; im=Image.new('RGBA',(S,S),(11,13,16,255)); d=ImageDraw.Draw(im)
    if not maskable:
        im=Image.new('RGBA',(S,S),(0,0,0,0)); d=ImageDraw.Draw(im); d.rounded_rectangle([0,0,S-1,S-1],radius=int(S*0.22),fill=(11,13,16,255))
    pad = 0.22 if maskable else 0.14
    pts=[(pad,0.66),(0.40,0.44),(0.56,0.56),(1-pad,0.28)]
    P=[(x*S,y*S) for x,y in pts]
    d.line(P,fill=(34,197,94,255),width=int(S*0.075),joint='curve')
    for x,y in P: r=S*0.037; d.ellipse([x-r,y-r,x+r,y+r],fill=(34,197,94,255))
    try: f=ImageFont.truetype('/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',int(S*0.16))
    except Exception: f=ImageFont.load_default()
    t='BTC 5M'; w=d.textlength(t,font=f); d.text(((S-w)/2,S*(0.70 if maskable else 0.72)),t,font=f,fill=(229,231,235,255))
    return im.resize((size,size),Image.LANCZOS)
icon(180).convert('RGB').save('icons/apple-touch-icon.png')  # iOS ignores transparency
icon(192).save('icons/icon-192.png'); icon(512).save('icons/icon-512.png'); icon(512,True).save('icons/icon-maskable-512.png'); icon(32).save('icons/favicon-32.png')
print('icons ok')
