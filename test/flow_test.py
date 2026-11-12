import asyncio, json, os
from playwright.async_api import async_playwright
URL='file://'+os.path.abspath('index.html')
async def run(p, nohist, shot):
    b=await p.chromium.launch(); pg=await b.new_page(viewport={'width':390,'height':844}, device_scale_factor=2)
    errs=[]; pg.on('pageerror', lambda e: errs.append(str(e))); pg.on('console', lambda m: errs.append(m.text) if m.type=='error' else None)
    if nohist: await pg.add_init_script('window.__noHist=true')
    await pg.add_init_script(path='test/mock.js'); await pg.goto(URL)
    seen=set(); texts={}
    for i in range(16):
        await pg.wait_for_timeout(1000)
        lab=await pg.inner_text('#recLabel'); act=await pg.inner_text('#action'); seen.add(lab); texts[lab]=act
    tgt=await pg.evaluate("()=>[window.__btc5m.TG.value>0, window.__btc5m.TG.src, window.__btc5m.TG.verified]")
    await pg.screenshot(path=shot); await b.close()
    return {'labels':sorted(seen),'examples':texts,'target':tgt,'errors':errs}
async def main():
    async with async_playwright() as p:
        print('NORMAL', json.dumps(await run(p, False, 'test/shots/flow_normal.png'), indent=1))
        print('MID-WINDOW', json.dumps(await run(p, True, 'test/shots/flow_mid.png'), indent=1))
asyncio.run(main())
