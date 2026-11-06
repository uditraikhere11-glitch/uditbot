"""Standalone no-dependency Telegram Message Designer for the existing Setup WebUI.

All client-authored values are assigned with textContent/value, never innerHTML.
The preview shows illustrative data; the saved server template uses real event data.
"""
from html import escape

PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="csrf-token" content="__CSRF__">
<title>AuditBot — Telegram Message Designer</title>
<style>
:root{color-scheme:dark;--bg:#0b1323;--card:#152138;--line:#34445f;--text:#f2f5fa;--soft:#afc1db;--blue:#7eb8ff;--accent:#4c92e9}
*{box-sizing:border-box}body{background:var(--bg);color:var(--text);font:15px/1.55 system-ui,-apple-system,sans-serif;margin:0}
main{max-width:1210px;margin:auto;padding:22px}.top{display:flex;justify-content:space-between;gap:18px;align-items:center;flex-wrap:wrap}
h1{font-size:26px;margin:0}h2{font-size:18px;margin:0 0 10px}h3{font-size:15px;margin:12px 0 8px}
p{margin:6px 0 14px;color:var(--soft)}a{color:var(--blue)}label{display:block;font-weight:600;margin:10px 0 5px}
input,select,textarea,button{font:inherit;max-width:100%}input[type=text],textarea,select{background:#0e192c;color:var(--text);border:1px solid var(--line);border-radius:9px;padding:9px;width:100%}
input[type=checkbox]{width:18px;height:18px;accent-color:var(--accent)}
button{cursor:pointer;border-radius:9px;padding:9px 13px;border:1px solid var(--line);background:#253653;color:var(--text)}
button:hover{background:#334970}button.primary{background:var(--accent);border-color:var(--accent);color:#071425;font-weight:750}
button:disabled{opacity:.5;cursor:not-allowed}button.small{padding:5px 8px;font-size:12px}
.grid{display:grid;grid-template-columns:minmax(0,1.15fr) minmax(310px,.85fr);gap:18px;margin-top:16px}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:20px;min-width:0}
fieldset{border:none;margin:0;padding:0}.stack{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.twocol{display:grid;grid-template-columns:1fr 1fr;gap:12px}.hint{color:var(--soft);font-size:12px}.control{margin-bottom:12px}
.row{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.row label{margin:0}.row .grow{flex:1;min-width:140px}
.fieldrow{display:grid;grid-template-columns:32px minmax(90px,1.2fr) 75px minmax(75px,.7fr) auto auto;gap:6px;align-items:center;padding:8px 2px;border-bottom:1px solid var(--line)}
.fieldrow .drag{color:var(--soft);cursor:grab;text-align:center}
.fieldrow input,.fieldrow select{padding:6px}.fieldrow button{padding:6px}
.option{display:flex;align-items:center;justify-content:space-between;gap:8px;background:#101b30;border-radius:7px;padding:7px 10px;margin:4px}
.option button{padding:4px 8px}#available{display:grid;grid-template-columns:1fr 1fr;gap:3px}
#preview{white-space:pre-wrap;overflow-wrap:anywhere;background:#0d1c31;border:1px solid var(--line);border-radius:12px;padding:16px;line-height:1.8;min-height:215px}
.immutable{border-left:3px solid #eab308;background:#1d2740;padding:10px;font-size:13px;color:#ecdfb4;margin:12px 0}
#feedback{min-height:24px;font-size:14px;color:#b4dec4}#feedback.error{color:#ffaca8}
.toolbar{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.toolbar button{flex-shrink:0}
.tip{font-size:12px;color:var(--soft);margin-top:8px}
#emoji-palette{display:flex;flex-wrap:wrap;gap:6px;margin-top:6px}
#emoji-palette button{padding:6px;min-width:35px;font-size:17px}
@media(max-width:850px){.grid{grid-template-columns:1fr}.twocol{grid-template-columns:1fr}}
@media(max-width:510px){.fieldrow{grid-template-columns:22px 1fr 65px;grid-auto-rows:auto}.fieldrow button{padding:5px}#available{grid-template-columns:1fr}}
</style>
</head>
<body>
<main>
  <div class="top">
    <div><h1>🎨 Telegram Message Designer</h1>
      <p>Choose fields, order, wording and emojis. Preview before saving. No restart required.</p></div>
    <a href="/">← AuditBot Setup</a>
  </div>
  <div class="immutable">🔒 Broker execution status and essential risk notices are fixed by the bot. Custom titles, text and emojis cannot turn an unfilled order into a verified fill. Templates are opt-in; existing notifications remain unchanged until enabled.</div>
  <div class="grid">
    <section class="card" aria-label="Message customization">
      <label for="event">Message type</label><select id="event"></select>
      <div class="row" style="margin-top:13px">
        <input type="checkbox" id="enabled">
        <label for="enabled">Enable custom formatting for this message type</label>
      </div>
      <p class="hint">Disable to use the existing AuditBot format for this category.</p>
      <div class="twocol">
        <div><label for="style">Layout</label>
          <select id="style"><option value="compact">Compact · two fields per line</option>
            <option value="standard">Standard · one field per line</option><option value="detailed">Detailed · more spacing</option></select></div>
        <div><label for="separator">Divider</label>
          <select id="separator"><option value="none">None</option><option value="line">Solid line</option>
          <option value="dots">Dots</option></select></div>
      </div>
      <label for="title">Custom heading</label><input id="title" type="text" maxlength="85">
      <div class="twocol">
        <div><label for="title-emoji">Heading emoji</label><select id="title-emoji"></select></div>
        <div><label for="custom-title-emoji">Type/paste your own emoji</label><input id="custom-title-emoji" maxlength="16" type="text" placeholder="🌟"></div>
        <div><label for="status-emoji">Status-line emoji</label><select id="status-emoji"></select></div>
        <div><label for="custom-status-emoji">Custom status emoji</label><input id="custom-status-emoji" maxlength="16" type="text" placeholder="✅"></div>
      </div>
      <div id="emoji-palette" aria-label="Suggested emojis"></div>
      <p class="hint">Tap an emoji to set the heading emoji, or type/paste any Unicode emoji. Telegram-only premium emoji IDs are not required or supported here.</p>
      <h3>Included fields <span class="hint" id="field-count"></span></h3>
      <p class="hint">Drag rows to reorder or use ↑ / ↓. Rename each label and choose an emoji independently.</p>
      <div id="selected" aria-label="Included fields"></div>
      <h3>Available fields</h3><div id="available"></div>
      <label for="footer">Optional custom note</label>
      <textarea id="footer" rows="2" maxlength="240" placeholder="Your own short note ..."></textarea>
      <p class="hint">The bot appends mandatory broker-status and safety notices after your note.</p>
      <div class="toolbar" style="margin-top:18px">
        <button type="button" id="preset-compact">Minimal preset</button>
        <button type="button" id="preset-standard">Standard preset</button>
        <button type="button" id="preset-detailed">All fields preset</button>
        <button type="button" id="reset">Reset this type</button>
      </div>
      <div class="toolbar" style="margin-top:16px">
        <button type="button" id="save" class="primary">Save & Apply Templates</button>
        <button type="button" id="export">Copy settings JSON</button>
        <button type="button" id="import">Import settings JSON</button>
      </div>
      <textarea id="import-json" hidden rows="5" placeholder="Paste previously exported settings JSON"></textarea>
      <button id="apply-import" type="button" hidden>Load pasted JSON for preview</button>
      <p id="feedback" role="status" aria-live="polite"></p>
    </section>
    <aside class="card" aria-label="Message preview">
      <h2>📱 Telegram Live Preview</h2>
      <p>Illustrative values only; actual alerts use broker-verified values.</p>
      <div id="preview" aria-live="polite"></div>
      <p class="tip" id="preview-note"></p>
      <div class="immutable">Mandatory safety labels remain visible even if every optional field is removed. Changes affect only newly formatted notifications, not historic Telegram messages.</div>
    </aside>
  </div>
</main>
<script>
(function() {
"use strict";
var csrf = document.querySelector('meta[name="csrf-token"]').content;
var state = null, original = null, defs = null, labels = null, emojiList = [], samples = {}, current = "fno_amo";
var $ = function(id){return document.getElementById(id)};
function clone(x){return JSON.parse(JSON.stringify(x))}
function feedback(msg,bad){$("feedback").textContent=msg;$("feedback").className=bad?"error":""}
function cfg(){return state.templates[current]}
function allowed(){return defs[current].allowed}
function emojiSelect(el,currentValue){
  el.replaceChildren();
  emojiList.forEach(function(e){
    var o=document.createElement("option");o.value=e;o.textContent=e||"None";el.appendChild(o);
  });
  if(!emojiList.includes(currentValue)){
    var opt=document.createElement("option");opt.value=currentValue;opt.textContent=currentValue+" (custom)";el.appendChild(opt);
  }
  el.value=currentValue;
}
function ensureField(key){
  var x=cfg().fields.find(function(f){return f.key===key});
  if(!x){x={key:key,label:labels[key],emoji:"•"};cfg().fields.push(x)}
  return x;
}
function move(index,step){
  var to=index+step;if(to<0||to>=cfg().fields.length)return;
  var arr=cfg().fields;var a=arr[index];arr[index]=arr[to];arr[to]=a;renderFields();preview();
}
function renderFields(){
  var list=$("selected");list.replaceChildren();
  cfg().fields.forEach(function(f,i){
    var row=document.createElement("div");row.className="fieldrow";row.draggable=true;row.dataset.index=String(i);
    var grip=document.createElement("span");grip.className="drag";grip.textContent="⠿";grip.title="Drag to reorder";row.appendChild(grip);
    var label=document.createElement("input");label.type="text";label.value=f.label;label.maxLength=40;label.title="Label for "+f.key;
    label.addEventListener("input",function(){f.label=label.value;preview()});row.appendChild(label);
    var em=document.createElement("select");emojiSelect(em,f.emoji);
    em.addEventListener("change",function(){f.emoji=em.value;custom.value=f.emoji;preview()});row.appendChild(em);
    var custom=document.createElement("input");custom.type="text";custom.maxLength=16;custom.value=f.emoji;custom.placeholder="Custom emoji";custom.title="Custom emoji";
    custom.addEventListener("input",function(){f.emoji=custom.value;emojiSelect(em,f.emoji);preview()});row.appendChild(custom);
    var up=document.createElement("button");up.type="button";up.className="small";up.textContent="↑";up.title="Move up";up.disabled=i===0;up.onclick=function(){move(i,-1)};row.appendChild(up);
    var down=document.createElement("button");down.type="button";down.className="small";down.textContent="↓";down.title="Move down";down.disabled=i===cfg().fields.length-1;down.onclick=function(){move(i,1)};row.appendChild(down);
    var remove=document.createElement("button");remove.type="button";remove.className="small";remove.textContent="×";remove.title="Remove field";remove.onclick=function(){cfg().fields.splice(i,1);renderFields();preview()};row.appendChild(remove);
    row.addEventListener("dragstart",function(e){e.dataTransfer.setData("text/plain",String(i));e.dataTransfer.effectAllowed="move"});
    row.addEventListener("dragover",function(e){e.preventDefault()});
    row.addEventListener("drop",function(e){e.preventDefault();var from=Number(e.dataTransfer.getData("text/plain"));var to=i;if(Number.isInteger(from)&&from>=0&&from<cfg().fields.length&&from!==to){var f=cfg().fields.splice(from,1)[0];cfg().fields.splice(to,0,f);renderFields();preview()}});
    list.appendChild(row);
  });
  $("field-count").textContent="("+cfg().fields.length+"/"+allowed().length+")";
  var available=$("available");available.replaceChildren();
  allowed().filter(function(k){return !cfg().fields.some(function(f){return f.key===k})}).forEach(function(key){
    var row=document.createElement("div");row.className="option";
    var name=document.createElement("span");name.textContent=labels[key];row.appendChild(name);
    var add=document.createElement("button");add.type="button";add.textContent="+ Add";add.onclick=function(){ensureField(key);renderFields();preview()};row.appendChild(add);
    available.appendChild(row);
  });
}
function bindFields(){
  ["enabled","style","separator","title","footer"].forEach(function(id){
    var el=$(id);el.addEventListener(id==="enabled"?"change":"input",function(){
      cfg()[id]=id==="enabled"?el.checked:el.value;preview();
    });
  });
  [["title-emoji","custom-title-emoji","title_emoji"],["status-emoji","custom-status-emoji","status_emoji"]].forEach(function(item){
    $(item[0]).onchange=function(){cfg()[item[2]]=$(item[0]).value;$(item[1]).value=cfg()[item[2]];preview()};
    $(item[1]).oninput=function(){cfg()[item[2]]=$(item[1]).value;emojiSelect($(item[0]),cfg()[item[2]]);preview()};
  });
}
function drawType(){
  var x=cfg();$("enabled").checked=x.enabled;$("style").value=x.style;$("separator").value=x.separator;
  $("title").value=x.title;$("footer").value=x.footer;
  [["title-emoji","custom-title-emoji","title_emoji"],["status-emoji","custom-status-emoji","status_emoji"]].forEach(function(item){
    emojiSelect($(item[0]),x[item[2]]);$(item[1]).value=x[item[2]];
  });
  renderFields();preview();
}
var mandatory = {
  equity_amo:["✅ AMO Accepted by Dhan","⏳ Queued for market OPEN — NOT EXECUTED","A separate confirmation will follow only after a broker-verified fill or final rejection/cancellation."],
  fno_amo:["✅ AMO Accepted by Dhan","⏳ Queued for market OPEN — NOT EXECUTED","A separate confirmation will follow only after a broker-verified fill or final rejection/cancellation.","⚠️ F&O AMO: No automatic SL/target."],
  submitted:["⏳ Status: Order Submitted","Awaiting broker confirmation of execution."],
  rejected:["❌ Status: Order Rejected","Dhan did not acknowledge the order."],
  executed:["✅ Status: Order Executed","Filled: 75/75","🛡️ Protection: Pending Broker Verification"],
  partial:["⏳ Status: Partially Executed","Filled: 25/75","🛡️ Protection: Pending Broker Verification"],
  final:["❌ Status: Order CANCELLED","Filled: 0/75"],
  unverified:["⚠️ Status: Fill Verification Pending","No confirmed execution yet."]
};
function preview(){
  if(!state||!defs)return;
  var x=cfg(),lines=[],entries=[];
  lines.push((x.title_emoji?x.title_emoji+" ":"")+x.title);
  x.fields.forEach(function(f){var val=samples[f.key];if(val!==undefined&&val!==null&&val!=="")entries.push((f.emoji?f.emoji+" ":"")+f.label+": "+val)});
  if(x.style==="compact"){for(var i=0;i<entries.length;i+=2)lines.push(entries.slice(i,i+2).join(" | "))}
  else lines=lines.concat(entries);
  if(x.separator==="line")lines.push("─────────────");
  if(x.separator==="dots")lines.push("••••••••••••");
  if(x.style==="detailed")lines.push("");
  if(x.footer)lines.push(x.footer);
  var locked=mandatory[current].slice();
  if(locked.length&&x.status_emoji){
    // Replace only the primary status icon, never the mandatory status text.
    locked[0]=x.status_emoji+" "+locked[0].replace(/^(✅|❌|⏳|⚠️)\s*/u,"");
  }
  $("preview").textContent=lines.concat(locked).join("\n");
  $("preview-note").textContent=x.enabled?"Custom format enabled for new "+current.replace(/_/g," ")+" notifications.":"Custom format OFF — preview shows how it will look if enabled.";
}
function preset(kind){
  var x=cfg();var keys=kind==="minimal"?defs[current].fields.slice(0,3):
    kind==="all"?allowed():defs[current].fields;
  x.fields=keys.map(function(key){return {key:key,label:labels[key],emoji:"•"}});
  x.style=kind==="minimal"?"compact":kind==="all"?"detailed":"standard";
  drawType();
}
function setType(type){current=type;drawType();feedback("")}
async function getJSON(url,init){
  var response=await fetch(url,init||{cache:"no-store"});
  var data=await response.json();if(!response.ok)throw new Error(data.error||data.detail||"HTTP "+response.status);return data;
}
async function load(){
  try{
    var data=await getJSON("/setup/telegram-designer/config");
    state=data.settings;original=data.defaults;defs=data.events;labels=data.fields;emojiList=data.emojis;samples=data.samples;
    Object.keys(defs).forEach(function(key){
      var opt=document.createElement("option");opt.value=key;opt.textContent=key.replace(/_/g," ").toUpperCase();$("event").appendChild(opt);
    });$("event").value=current;
    emojiList.filter(Boolean).forEach(function(e){
      var b=document.createElement("button");b.type="button";b.title="Use as heading emoji";b.textContent=e;b.onclick=function(){cfg().title_emoji=e;drawType()};$("emoji-palette").appendChild(b);
    });
    drawType();feedback("Settings loaded; no changes have been saved.");
  }catch(e){feedback("Could not load settings: "+e.message,true);$("save").disabled=true}
}
$("event").onchange=function(){setType($("event").value)};
["preset-compact","preset-standard","preset-detailed"].forEach(function(id,i){$(id).onclick=function(){preset(["minimal","standard","all"][i])}});
$("reset").onclick=function(){state.templates[current]=clone(original.templates[current]);drawType();feedback("Type reset in preview; select Save to apply.")};
$("save").onclick=async function(){
  $("save").disabled=true;feedback("Validating and saving...");
  try{
    var data=await getJSON("/setup/telegram-designer/save",{
      method:"POST",headers:{"Content-Type":"application/json","X-CSRF-Token":csrf},
      body:JSON.stringify(state),credentials:"same-origin"
    });
    state=data.settings;feedback("✅ Saved. New notifications will use enabled templates; no restart needed.");drawType();
  }catch(e){feedback("Save rejected: "+e.message,true)}
  finally{$("save").disabled=false}
};
$("export").onclick=async function(){
  try{await navigator.clipboard.writeText(JSON.stringify(state,null,2));feedback("Settings JSON copied.")}
  catch(e){feedback("Clipboard unavailable. Use the import/export textbox instead.",true);
    $("import-json").hidden=false;$("import-json").value=JSON.stringify(state,null,2)}
};
$("import").onclick=function(){$("import-json").hidden=false;$("apply-import").hidden=false;$("import-json").focus()};
$("apply-import").onclick=function(){
  try{var candidate=JSON.parse($("import-json").value);
      if(!candidate||candidate.version!==1||!candidate.templates||Object.keys(candidate.templates).length!==Object.keys(defs).length)throw Error("Wrong template schema");
      state=candidate;drawType();feedback("Imported into preview; select Save to validate and apply.");}
  catch(e){feedback("Invalid import: "+e.message,true)}
};
bindFields();load();
})();
</script>
</body></html>"""


def render_page(csrf_token: str) -> str:
    return PAGE.replace("__CSRF__", escape(csrf_token, quote=True))
