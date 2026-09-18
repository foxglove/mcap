// cspell:words Segoe Consolas
export const inspectorStyles = `:host {
  display: block;
  position: relative;
  container-type: inline-size;
  font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
  color:#dce6f2;
  background:#0c1117;
  font-synthesis:none;
  color-scheme:dark;
  font-size:14px
}
* {
  box-sizing:border-box
}
body {
  margin:0
}
button,input,select {
  font:inherit
}
button {
  color:#c9d7e7;
  background:#1b2531;
  border:1px solid #334254;
  border-radius:6px;
  padding:8px 12px;
  cursor:pointer;
  white-space:nowrap
}
button:hover {
  background:#293748;
  border-color:#59728e
}
button:focus-visible,input:focus-visible,select:focus-visible,canvas:focus-visible {
  outline:2px solid #79baff;
  outline-offset:2px
}
button.primary {
  background:#8dc3ff;
  color:#101820;
  border-color:#8dc3ff;
  font-weight:650
}
button.primary:hover {
  background:#b2d7ff
}
.text-button {
  background:transparent;
  border:0;
  color:#a6caff
}
header {
  height:76px;
  border-bottom:1px solid #26313d;
  display:flex;
  align-items:center;
  justify-content:space-between;
  padding:0 26px;
  gap:20px
}
.brand {
  display:flex;
  align-items:center;
  gap:12px;
  white-space:nowrap
}
.brand-icon {
  color:#8dc3ff;
  border:1px solid #3c6289;
  border-radius:7px;
  width:34px;
  height:34px;
  display:grid;
  place-items:center;
  font-size:25px
}
h1 {
  font-size:18px;
  letter-spacing:-.5px;
  font-weight:700;
  margin:0
}
h1 span {
  font-weight:400;
  color:#94a7bc;
  border-left:1px solid #354354;
  margin-left:14px;
  padding-left:14px;
  letter-spacing:0
}
.header-actions {
  display:flex;
  align-items:center;
  gap:10px
}
.local {
  font-size:12px;
  color:#859bb1;
  margin-right:10px
}
main {
  padding:24px 26px 0
}
.file-bar {
  display:flex;
  justify-content:space-between;
  align-items:center;
  gap:20px;
  min-height:66px;
  margin-bottom:22px
}
.eyebrow {
  font-size:11px;
  font-weight:600;
  letter-spacing:1.6px;
  color:#7e93aa
}
#filename {
  font-size:19px;
  margin-top:7px;
  overflow-wrap:anywhere
}
.stats {
  display:flex;
  gap:24px;
  flex-wrap:wrap;
  color:#7e94ab;
  font-size:12px
}
.stats span {
  display:flex;
  flex-direction:column;
  gap:5px
}
.stats strong {
  font-family:ui-monospace,SFMono-Regular,Consolas,monospace;
  font-size:17px;
  font-weight:500;
  color:#dde9f7
}
.toolbar {
  display:flex;
  align-items:center;
  gap:16px;
  background:#131b24;
  padding:12px;
  border:1px solid #293544;
  border-radius:8px 8px 0 0;
  flex-wrap:wrap
}
.search {
  display:flex;
  align-items:center;
  gap:8px;
  border:1px solid #334152;
  background:#0e151d;
  border-radius:5px;
  padding:0 10px;
  max-width:100%
}
.search span {
  font-size:25px;
  color:#758da6
}
input:not([type=range]):not([type=file]) {
  background:transparent;
  border:0;
  color:#dde9f7;
  padding:8px 0;
  min-width:0;
  width:208px
}
input::placeholder {
  color:#7c90a6
}
.legend {
  display:flex;
  gap:16px;
  margin-left:10px;
  color:#9eafc2;
  font-size:12px
}
.legend span {
  display:flex;
  align-items:center;
  gap:7px
}
.legend i {
  display:block
}
.tick {
  width:1px;
  height:12px;
  background:#d7e5f3
}
.chunk {
  width:16px;
  height:12px;
  background:#5aafff30;
  border:1px solid #5aafff
}
.loose {
  width:2px;
  height:12px;
  background:#ffb570
}
.view-controls {
  display:flex;
  align-items:center;
  gap:6px;
  margin-left:auto
}
.view-controls label {
  font-size:12px;
  color:#8ba0b7;
  margin-right:8px
}
select {
  background:#1b2531;
  color:#cfdaea;
  border:1px solid #334254;
  border-radius:5px;
  padding:7px;
  max-width:100%
}
.view-controls button {
  padding:7px 10px
}
.workspace {
  display:flex;
  border:1px solid #293544;
  border-top:0;
  height:var(--mcap-inspector-height, 520px);
  min-height:0
}
.plot {
  flex:1;
  position:relative;
  min-width:0;
  overflow:hidden
}
canvas {
  display:block;
  width:100%;
  height:100%;
  cursor:grab;
  touch-action:none
}
aside {
  width:270px;
  flex-shrink:0;
  background:#111820;
  border-left:1px solid #2b3542;
  padding:16px 18px;
  overflow:auto
}
.inspector-heading {
  display:flex;
  align-items:center;
  justify-content:space-between
}
.inspector-heading button {
  background:transparent;
  border:0;
  padding:0 4px;
  color:#8299b1;
  font-size:22px
}
h2 {
  font-size:18px;
  font-weight:550;
  letter-spacing:-.3px
}
h3 {
  font-size:15px;
  margin:24px 0 10px
}
p {
  font-size:14px;
  line-height:1.7;
  color:#a0b1c4
}
.muted {
  color:#71869e;
  font-size:12px
}
.sample-colors {
  display:flex;
  gap:4px;
  margin:24px 0
}
.sample-colors i {
  display:block;
  width:28px;
  height:4px;
  border-radius:1px
}
dl {
  margin:16px 0
}
dl>div {
  margin:0 0 12px
}
dt {
  font-size:11px;
  color:#7d91a8;
  margin-bottom:4px
}
dd {
  font:12px/1.5 ui-monospace,SFMono-Regular,Consolas,monospace;
  margin:0;
  overflow-wrap:anywhere;
  color:#cfdfef
}
.accessible {
  padding-top:18px;
  border-top:1px solid #263341;
  margin-top:24px
}
.accessible label {
  display:block;
  color:#899fb6;
  font-size:12px;
  margin-bottom:8px
}
.accessible select {
  width:100%;
  font-size:12px
}
.navigator {
  border:1px solid #293544;
  border-top:0;
  background:#131b24;
  padding:13px 16px;
  display:flex;
  align-items:center;
  gap:14px;
  border-radius:0 0 8px 8px;
  font:12px ui-monospace,SFMono-Regular,Consolas,monospace;
  color:#8fa6be
}
.navigator input {
  flex:1;
  min-width:30px;
  accent-color:#70b3ff;
  height:14px
}
.navigator span {
  white-space:nowrap
}
#window-size {
  border-left:1px solid #334153;
  padding-left:14px;
  color:#bdd0e5
}
footer {
  display:flex;
  justify-content:space-between;
  gap:20px;
  color:#71869d;
  font-size:11px;
  padding:15px 0;
  line-height:1.7
}
footer b {
  padding:0 7px;
  font-weight:400;
  color:#3c536c
}
#status {
  color:#95a9c0;
  text-align:right
}
.empty,.loading {
  position:absolute;
  inset:0;
  display:flex;
  flex-direction:column;
  align-items:center;
  justify-content:center;
  background:#10151bf5;
  text-align:center;
  padding:24px
}
.empty {
  padding-left:min(10%,80px)
}
.empty h2 {
  font-size:25px;
  margin:20px 0 0
}
.empty p {
  margin:12px 0 22px
}
.empty small {
  color:#6e859e;
  font-size:12px;
  margin-top:24px
}
.empty-mark {
  font-size:48px;
  color:#7db4ed
}
.loading-card {
  width:min(360px,100%)
}
.loading-card h2 {
  overflow-wrap:anywhere
}
.loading-card progress {
  width:100%;
  accent-color:#8dc3ff;
  height:8px
}
.tooltip {
  position:absolute;
  pointer-events:none;
  z-index:3;
  background:#263547;
  border:1px solid #59738d;
  border-radius:5px;
  padding:8px 10px;
  font:12px/1.5 ui-monospace,SFMono-Regular,Consolas,monospace;
  max-width:min(460px,90%);
  box-shadow:0 4px 20px #0006
}
#error {
  background:#372127;
  border:1px solid #8c4855;
  padding:12px 16px;
  color:#ffb9b9;
  overflow-wrap:anywhere
}
#drop-overlay {
  position:absolute;
  inset:12px;
  z-index:10;
  border:2px dashed #8dc3ff;
  border-radius:12px;
  background:#12243bef;
  display:grid;
  place-items:center;
  text-align:center;
  pointer-events:none
}
#drop-overlay span {
  font-size:52px;
  color:#8dc3ff
}
#drop-overlay h2 {
  font-size:28px
}
[hidden] {
  display:none!important
}
@container(min-width:1600px) {
  aside {
  width:300px
}
.workspace {
  min-height:0
}

}
@container(max-width:1050px) {
  .local {
  display:none
}
.stats {
  gap:18px
}
aside {
  width:235px
}
.legend {
  margin-left:0;
  gap:10px
}
.view-controls {
  margin-left:0
}
.toolbar {
  gap:10px
}
.workspace {
  height:var(--mcap-inspector-height, 520px)
}

}
@container(max-width:760px) {
  header {
  height:auto;
  min-height:72px;
  padding:16px;
  flex-wrap:wrap;
  gap:12px
}
h1 {
  font-size:16px
}
main {
  padding:16px 12px 0
}
.file-bar {
  align-items:flex-start;
  flex-direction:column;
  margin-bottom:16px;
  gap:16px
}
.stats {
  gap:18px;
  flex-wrap:wrap
}
.stats strong {
  font-size:14px
}
.workspace {
  height:auto;
  display:block
}
.plot {
  height:var(--mcap-inspector-height, 520px)
}
aside {
  width:100%;
  border-left:0;
  border-top:1px solid #293544;
  max-height:320px
}
.toolbar {
  gap:12px
}
.view-controls {
  flex-wrap:wrap
}
.legend {
  margin-left:0
}
.search {
  flex:1
}
.search input {
  width:100%
}
.empty h2 {
  font-size:21px
}
.navigator {
  gap:8px;
  padding:12px 8px;
  font-size:11px
}
#window-size {
  display:none
}
footer {
  flex-direction:column;
  gap:4px
}
#status {
  text-align:left
}

}

.grouping { display: flex; align-items: center; gap: 0; }
.grouping > span { margin-right: 8px; color: #9eafc2; font-size: 12px; white-space: nowrap; }
.grouping button { border-radius: 0; padding: 7px 10px; }
.grouping button:first-of-type { border-radius: 5px 0 0 5px; }
.grouping button:last-of-type { border-radius: 0 5px 5px 0; margin-left: -1px; }
.grouping button[aria-pressed="true"] { background: #294b6e; border-color: #699cd0; color: #e4f0ff; z-index: 1; }
.group-hint { padding: 9px 14px; color: #99afc7; background: #111c28; border: 1px solid #293544; border-top: 0; font-size: 12px; }

.group-hint { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; }
.group-hint span { margin-right: auto; }
.group-hint button { font-size: 12px; padding: 4px 8px; }
.channel-picker { display:flex; flex-direction:column; gap:8px; margin:18px 0; color:#a8bdd2; font-size:12px; }
.overview { position:relative; flex:1; min-width:80px; height:28px; }
.overview-track { position:absolute; inset:8px 0; background:#263647; border-radius:3px; overflow:hidden; }
.viewport-range { position:absolute; top:0; bottom:0; background:#77b9ff; border:1px solid #d1e8ff; border-radius:2px; }
.overview input { position:absolute; inset:0; width:100%; height:100%; margin:0; appearance:none; background:transparent; cursor:ew-resize; }
.overview input::-webkit-slider-runnable-track { background:transparent; }
.overview input::-moz-range-track, .overview input::-moz-range-progress { background:transparent; }
.overview input::-webkit-slider-thumb { appearance:none; width:14px; height:24px; background:transparent; }
.overview input::-moz-range-thumb { width:14px; height:24px; border:0; background:transparent; }
.window-loading { position:absolute; top:50%; left:50%; transform:translate(-50%,-50%); width:min(320px,90%); padding:16px; display:flex; flex-direction:column; gap:10px; text-align:center; border:1px solid #526f8d; border-radius:8px; background:#182a3fee; color:#d4e7fc; pointer-events:none; box-shadow:0 4px 20px #0006; }
.window-loading progress { width:100%; accent-color:#8dc3ff; }
button[aria-pressed=true] { border-color:#7ebdff; background:#26435f; }
`;
