import asyncio, json, sys
from playwright.async_api import async_playwright
async def main():
    async with async_playwright() as p:
        b = await p.chromium.launch()
        pg = await b.new_page(viewport={'width':390,'height':844}, device_scale_factor=2)
        errs=[]
        pg.on('console', lambda m: errs.append(m.text) if m.type=='error' else None)
        pg.on('pageerror', lambda e: errs.append('PAGEERROR '+str(e)))
        await pg.add_init_script(path='test/mock.js')
        await pg.goto('file://'+__import__('os').path.abspath('index.html')+'')
        await pg.wait_for_timeout(9000)
        s = await pg.evaluate("""()=>{const a=window.__btc5m;const d=a.decision;return {rec:d.rec,pUp:d.pUp,pModel:d.pModel,gates:d.gates,text:d.text,target:a.TG,S:a.st.S,sSrc:a.st.sSrc,feeds:Object.fromEntries(Object.entries(a.X).map(([k,f])=>[k,[f.status,f.msgs,f.fresh]])),mk:[a.MK.slug,a.MK.up.ask,a.MK.down.ask,a.MK.gamma],candles:a.candles.length,preds:a.preds.length,papers:a.papers.length,diag:a.diagLines.slice(-12)}}""")
        print(json.dumps(s,indent=1))
        await pg.screenshot(path='test/shots/shot_live.png', full_page=True)
        for t in ['signals','paper','settings']:
            await pg.click(f'#nav button[data-t={t}]'); await pg.wait_for_timeout(1200)
            await pg.screenshot(path=f'test/shots/shot_{t}.png', full_page=True)
        await pg.click('#nav button[data-t=backtest]'); await pg.select_option('#btDays','3'); await pg.click('#runBt')
        for i in range(60):
            await pg.wait_for_timeout(1000); st=await pg.inner_text('#btStatus')
            if st.startswith('Done') or 'failed' in st: break
        print('BT:', st)
        await pg.screenshot(path='test/shots/shot_backtest.png', full_page=True)
        await pg.click('#nav button[data-t=live]'); await pg.click('#btnEditTarget'); await pg.fill('#inTarget','71500'); await pg.click('#saveTarget'); await pg.wait_for_timeout(1500)
        print('after manual target:', await pg.evaluate("()=>[window.__btc5m.TG.value, window.__btc5m.TG.src, window.__btc5m.decision.rec]"))
        await pg.click('#paperUp'); await pg.wait_for_timeout(600)
        print('papers:', await pg.evaluate("()=>window.__btc5m.papers.length"))
        print('ERRORS:', errs)
        await b.close()
asyncio.run(main())
