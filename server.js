// Nova Cidade RP - servidor online (sem dependências). Node 18+.
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const PORT=process.env.PORT||3000,MAXP=+(process.env.MAX_PLAYERS||50),NSRV=8,TICK=125,NEAR=350,MAXMSG=16384;
const INDEX=[path.join(__dirname,'index.html'),path.join(__dirname,'public','index.html')].find(p=>fs.existsSync(p))||path.join(__dirname,'index.html'),TOPICS=new Set(['chat','npc','hit']);
const rooms=new Map(); // nome -> {clients:Map(id->c), dirty:bool}
for(let i=1;i<=NSRV;i++)rooms.set('srv'+i,{clients:new Map(),dirty:false});

const server=http.createServer((req,res)=>{
  if(req.url==='/health'){res.writeHead(200,{'content-type':'text/plain'});return res.end('ok');}
  if(req.url==='/status'){const o={};for(const [n,r] of rooms)o[n]=r.clients.size+'/'+MAXP;res.writeHead(200,{'content-type':'application/json'});return res.end(JSON.stringify(o));}
  if(req.url==='/'||req.url.startsWith('/?')||req.url==='/index.html'){
    fs.readFile(INDEX,(e,b)=>{if(e){res.writeHead(500);return res.end('index.html não encontrado');}res.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-cache'});res.end(b);});return;}
  res.writeHead(404);res.end('404');
});

/* ---------- WebSocket (RFC 6455) feito à mão ---------- */
server.on('upgrade',(req,socket)=>{
  if(req.url!=='/ws'||!req.headers['sec-websocket-key']){socket.destroy();return;}
  const acc=crypto.createHash('sha1').update(req.headers['sec-websocket-key']+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+acc+'\r\n\r\n');
  socket.setNoDelay(true);
  const c={id:crypto.randomBytes(4).toString('hex'),socket,buf:Buffer.alloc(0),frag:null,room:null,pres:{},tok:60,tokT:Date.now(),alive:true};
  socket.on('data',d=>{c.buf=Buffer.concat([c.buf,d]);try{parse(c);}catch(e){kill(c);}});
  socket.on('close',()=>kill(c));socket.on('error',()=>kill(c));
});
function frame(op,payload){const n=payload.length;let h;
  if(n<126)h=Buffer.from([0x80|op,n]);else if(n<65536){h=Buffer.alloc(4);h[0]=0x80|op;h[1]=126;h.writeUInt16BE(n,2);}
  else{h=Buffer.alloc(10);h[0]=0x80|op;h[1]=127;h.writeBigUInt64BE(BigInt(n),2);}
  return Buffer.concat([h,payload]);}
function raw(c,buf){if(!c.socket.destroyed&&c.socket.writable)c.socket.write(buf);}
function send(c,obj){raw(c,frame(1,Buffer.from(typeof obj==='string'?obj:JSON.stringify(obj))));}
function parse(c){
  for(;;){const b=c.buf;if(b.length<2)return;const fin=!!(b[0]&0x80),op=b[0]&15,masked=!!(b[1]&0x80);let len=b[1]&127,off=2;
    if(!masked){kill(c);return;}
    if(len===126){if(b.length<4)return;len=b.readUInt16BE(2);off=4;}
    else if(len===127){if(b.length<10)return;const L=b.readBigUInt64BE(2);if(L>BigInt(MAXMSG)){kill(c);return;}len=Number(L);off=10;}
    if(len>MAXMSG){kill(c);return;}
    if(b.length<off+4+len)return;
    const mask=b.subarray(off,off+4),p=Buffer.from(b.subarray(off+4,off+4+len));for(let i=0;i<p.length;i++)p[i]^=mask[i&3];
    c.buf=b.subarray(off+4+len);
    if(op===8){kill(c);return;}
    if(op===9){raw(c,frame(10,p));continue;}
    if(op===10){c.alive=true;continue;}
    if(op===1||op===2){c.frag=[p];c.fragOp=op;}else if(op===0&&c.frag){c.frag.push(p);}else continue;
    if(c.frag.reduce((s,x)=>s+x.length,0)>MAXMSG){kill(c);return;}
    if(fin){const m=Buffer.concat(c.frag).toString('utf8');c.frag=null;if(c.fragOp===1)handle(c,m);}
  }}
function kill(c){if(c.dead)return;c.dead=true;leave(c);try{c.socket.destroy();}catch(e){}}

/* ---------- salas ---------- */
function leave(c){if(!c.room)return;const r=rooms.get(c.room);if(r){r.clients.delete(c.id);r.dirty=true;}c.room=null;c.pres={};}
function handle(c,txt){
  const now=Date.now();c.tok=Math.min(80,c.tok+(now-c.tokT)/1000*60);c.tokT=now;if(c.tok<1)return;c.tok--;
  let m;try{m=JSON.parse(txt);}catch(e){return;}if(!m||typeof m!=='object')return;
  if(m.t==='join'){const r=rooms.get(m.room);if(!r)return;leave(c);
    if(r.clients.size>=MAXP){send(c,{t:'full',room:m.room});return;}
    c.room=m.room;c.pres={};r.clients.set(c.id,c);r.dirty=true;send(c,{t:'joined',id:c.id,room:m.room});}
  else if(m.t==='pres'&&c.room){if(!m.d||typeof m.d!=='object'||Array.isArray(m.d))return;const s=JSON.stringify(m.d);if(s.length>700)return;
    for(const k of Object.keys(m.d)){if(k.length<=3)c.pres[k]=m.d[k];}rooms.get(c.room).dirty=true;}
  else if(m.t==='emit'&&c.room){if(!TOPICS.has(m.topic))return;const s=JSON.stringify(m.d===undefined?null:m.d);if(s.length>4500)return;
    const out=frame(1,Buffer.from('{"t":"ev","topic":'+JSON.stringify(m.topic)+',"from":"'+c.id+'","d":'+s+'}'));
    for(const o of rooms.get(c.room).clients.values())if(o!==c)raw(o,out);}
}
setInterval(()=>{
  for(const r of rooms.values()){if(!r.dirty)continue;r.dirty=false;const cl=[...r.clients.values()];
    const full=cl.map(p=>JSON.stringify(p.pres)),stub=cl.map(p=>JSON.stringify({jt:p.pres.jt}));
    for(const rc of cl){const hasPos=typeof rc.pres.x==='number';const parts=[];
      for(let i=0;i<cl.length;i++){const p=cl[i];let near=p===rc;
        if(!near&&hasPos&&typeof p.pres.x==='number')near=Math.hypot(p.pres.x-rc.pres.x,p.pres.z-rc.pres.z)<NEAR;
        parts.push('{"p":"'+p.id+'","d":'+(near?full[i]:stub[i])+'}');}
      raw(rc,frame(1,Buffer.from('{"t":"peers","l":['+parts.join(',')+']}')));}}
},TICK);
setInterval(()=>{for(const r of rooms.values())for(const c of r.clients.values()){if(!c.alive){kill(c);continue;}c.alive=false;raw(c,frame(9,Buffer.alloc(0)));}},30000);
server.listen(PORT,()=>console.log('Nova Cidade RP online na porta '+PORT));
