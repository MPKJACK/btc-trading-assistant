"""Builds the deployable site at the repository root (index.html, sw.js) from src/.
Run: python3 build.py   (CI runs it too and fails if the committed files are stale)."""
import hashlib, pathlib, sys
r = pathlib.Path(__file__).parent
html = (r/'src/index.html').read_text()
html = html.replace('/*ENGINE*/', (r/'src/engine.js').read_text()).replace('/*APP*/', (r/'src/app.js').read_text())
build = hashlib.sha256(html.encode()).hexdigest()[:10]
sw = (r/'src/sw.js').read_text().replace('__BUILD__', build)
out = {'index.html': html, 'sw.js': sw}
if '--check' in sys.argv:
    stale = [k for k, v in out.items() if not (r/k).exists() or (r/k).read_text() != v]
    if stale: print('Stale build output, run python3 build.py:', stale); sys.exit(1)
    print('build output up to date', build); sys.exit(0)
for k, v in out.items(): (r/k).write_text(v)
print('built', build, len(html), 'bytes')
