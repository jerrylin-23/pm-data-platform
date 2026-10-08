import { useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './style.css';

type Level = {price_e4:number;size_e6:number};
type Metadata = {parent_event_id?:string;event_title?:string;event_url?:string;market_label?:string;market_order?:number|string;market_type?:string;outcome_labels?:Record<string,string>};
type Market = Metadata & {venue:string;market_id:string;instrument_id:string;outcome:string;title:string;source_url:string;price:string|null;volume_24h:string};
type Book = Metadata & {key:string;venue:string;market_id:string;instrument_id:string;outcome:string;title:string;status:string;data_origin:string;continuity:string;bids:Level[];asks:Level[];version:number;recv_ts_ns:number;event_id:string;source_url?:string};
type Alert = {alert_id:string;pair:string;gap_e4:number;ask_e4:number;bid_e4:number;recv_ts_ns:number;source_event_ids:string[];continuity:string[];data_origin:string[]};
type State = {mode:string;demo:boolean;books:Book[];tracked:Market[];alerts:Alert[];violations:{reason:string;event_id?:string}[];kalshi_ready:boolean;server_ts_ns:number};
type MarketBook = {key:string;meta:Market|Book;books:Book[]};
type EventBook = {key:string;title:string;venue:string;source_url?:string;markets:MarketBook[]};
type OutcomeBook = Book & {derived?:boolean};
type HistoryPoint = {ts:number;values:Record<string,number|null>};

const price = (p:number|undefined) => p === undefined ? 'N/A' : (p / 10000).toFixed(4);
const probability = (p:number|null) => p === null ? 'N/A' : `${(p/100).toFixed(2)}%`;
const quantity = (q:number) => (q/1000000).toLocaleString(undefined,{maximumFractionDigits:6});
const venueName = (v:string) => v === 'polymarket' ? 'Polymarket' : v === 'kalshi' ? 'Kalshi' : v;
const marketKey = (m:{venue:string;market_id:string}) => `${m.venue}:${m.market_id}`;
const eventKey = (m:(Market|Book)) => m.parent_event_id?`${m.venue}:event:${m.parent_event_id}`:marketKey(m);
const choiceLabel = (m:MarketBook) => m.meta.market_label??m.meta.title;
const sideLabel = (m:MarketBook|undefined,side:string) => m?.meta.outcome_labels?.[side]??side.toUpperCase();
const marketType = (m:MarketBook) => m.meta.market_type??'markets';
const typeLabel = (type:string) => type.replaceAll('_',' ').replace(/\b[a-z]/g,c=>c.toUpperCase());
const seriesLabel = (m:MarketBook) => sideLabel(m,'yes').toLowerCase()==='yes'?choiceLabel(m):`${choiceLabel(m)} · ${sideLabel(m,'yes')}`;
const choiceColor = (index:number) => `hsl(${(155+index*137.508)%360} 55% 65%)`;
const midpoint = (book:OutcomeBook|undefined) => book?.status==='live'&&book.bids[0]&&book.asks[0] ? (book.bids[0].price_e4+book.asks[0].price_e4)/2 : null;
function outcomeBook(market:MarketBook|undefined,outcome:'yes'|'no'):OutcomeBook|undefined {
  const direct=market?.books.find(b=>b.outcome===outcome);
  if(direct)return direct;
  const other=market?.books.find(b=>b.outcome!==outcome);
  if(!other)return undefined;
  return {...other,key:`${other.key}:${outcome}`,outcome,derived:true,
    bids:other.asks.map(l=>({...l,price_e4:10000-l.price_e4})),
    asks:other.bids.map(l=>({...l,price_e4:10000-l.price_e4}))};
}
async function request(path:string, method='GET', body?:unknown) {
  const response=await fetch(path,{method,headers:{'Content-Type':'application/json'},body:body===undefined?undefined:JSON.stringify(body)});
  const data=await response.json();
  if(!response.ok) throw new Error(data.detail ?? 'Request failed.');
  return data;
}

function App(){
  const [state,setState]=useState<State|null>(null);
  const [connected,setConnected]=useState(false);
  const [selected,setSelected]=useState('');
  const [selectedChoice,setSelectedChoice]=useState('');
  const [query,setQuery]=useState('');
  const [results,setResults]=useState<Market[]>([]);
  const [chosen,setChosen]=useState<Set<string>>(new Set());
  const [notice,setNotice]=useState('');
  const [busy,setBusy]=useState(false);
  const [showSearch,setShowSearch]=useState(false);
  const [depthOutcome,setDepthOutcome]=useState<'yes'|'no'>('yes');
  const [session,setSession]=useState<{key:string;points:HistoryPoint[]}>({key:'',points:[]});

  useEffect(()=>{
    let disposed=false;let timer:ReturnType<typeof setTimeout>;let socket:WebSocket;
    const connect=()=>{
      socket=new WebSocket(`${location.protocol==='https:'?'wss':'ws'}://${location.host}/ws`);
      socket.onopen=()=>setConnected(true);
      socket.onmessage=message=>{
        const next=JSON.parse(message.data) as State;setState(next);
      };
      socket.onclose=()=>{setConnected(false);if(!disposed)timer=setTimeout(connect,1500);};
    };connect();return()=>{disposed=true;clearTimeout(timer);socket?.close();};
  },[]);
  const books=state?.books??[];
  const trackedKeys=new Set((state?.tracked??[]).map(m=>`${m.venue}:${m.instrument_id}`));
  const grouped=new Map<string,MarketBook>();
  for(const m of state?.tracked??[]){const key=marketKey(m);if(!grouped.has(key))grouped.set(key,{key,meta:m,books:[]});}
  for(const book of books){if(book.data_origin!=='synthetic'&&!trackedKeys.has(book.key))continue;const key=marketKey(book);if(!grouped.has(key)){if(book.data_origin!=='synthetic')continue;grouped.set(key,{key,meta:book,books:[]});}grouped.get(key)!.books.push(book);}
  const markets=[...grouped.values()];
  const eventGroups=new Map<string,EventBook>();
  for(const m of markets){const key=eventKey(m.meta);if(!eventGroups.has(key))eventGroups.set(key,{key,title:m.meta.event_title??m.meta.title,venue:m.meta.venue,source_url:m.meta.event_url??m.meta.source_url,markets:[]});eventGroups.get(key)!.markets.push(m);}
  const events=[...eventGroups.values()];
  for(const e of events)e.markets.sort((a,b)=>Number(a.meta.market_order??0)-Number(b.meta.market_order??0)||choiceLabel(a).localeCompare(choiceLabel(b),undefined,{numeric:true}));
  const event=eventGroups.get(selected)??events.find(e=>e.markets.some(m=>m.books.some(b=>b.data_origin==='live')) )??events[0];
  const market=event?.markets.find(m=>m.key===selectedChoice)??event?.markets[0];
  const types=[...new Set(event?.markets.map(marketType)??[])];
  const choices=event?.markets.filter(m=>marketType(m)===marketType(market!))??[];
  const chartKey=event?`${event.key}:${marketType(market!)}`:'';
  const yes=outcomeBook(market,'yes'),no=outcomeBook(market,'no');
  const book=depthOutcome==='yes'?yes:no;
  const multiple=choices.length>1;
  const labeledChoice=!!market&&(multiple||types.length>1||choiceLabel(market)!==event?.title);
  const series=multiple?choices.map((m,i)=>({key:m.key,label:seriesLabel(m),book:outcomeBook(m,'yes'),color:choiceColor(i)})):
    [{key:'yes',label:sideLabel(market,'yes'),book:yes,color:'#93cdb6'},{key:'no',label:sideLabel(market,'no'),book:no,color:'#d49697'}];
  const signature=series.map(s=>`${s.key}:${s.book?.version}:${s.book?.status}:${s.book?.recv_ts_ns}`).join('|');
  const history=session.key===chartKey?session.points:[];
  useEffect(()=>{
    if(!event||!series.some(s=>s.book))return;
    const point={ts:Math.max(...series.map(s=>s.book?.recv_ts_ns??0))/1000000,values:Object.fromEntries(series.map(s=>[s.key,midpoint(s.book)]))};
    setSession(previous=>({key:chartKey,points:[...(previous.key===chartKey?previous.points:[]),point].slice(-60)}));
  },[chartKey,signature]);

  async function search(){
    setBusy(true);setNotice('');setShowSearch(true);
    try{
      const data=/^[a-z][a-z0-9+.-]*:/i.test(query.trim())?await request('/api/resolve','POST',{url:query}):await request(`/api/search?q=${encodeURIComponent(query)}`);
      setResults(data.markets);setChosen(new Set());
      if(data.note)setNotice(data.note);
      if(data.errors?.length)setNotice(`${data.note??''} Some venue lookups failed. Try a direct market link.`);
      if(!data.markets.length)setNotice('No open markets found. Try a direct event link.');
    }catch(error){setNotice((error as Error).message);setResults([]);}finally{setBusy(false);}
  }
  async function track(markets:Market[]){
    setBusy(true);setNotice('');
    try{
      const groups=new Map<string,Set<string>>();
      for(const m of markets){if(!groups.has(m.source_url))groups.set(m.source_url,new Set());groups.get(m.source_url)!.add(m.market_id);}
      for(const [url,ids] of groups)await request('/api/track','POST',{url,market_ids:[...ids]});
      if(markets[0]){setSelected(eventKey(markets[0]));setSelectedChoice(marketKey(markets[0]));}
      setNotice('Tracking saved. The feed will request a fresh snapshot.');setShowSearch(false);
    }catch(error){setNotice((error as Error).message);}finally{setBusy(false);}
  }
  async function untrack(markets:Market[]){
    setBusy(true);
    try{for(const m of markets)await request(`/api/track/${encodeURIComponent(`${m.venue}:${m.instrument_id}`)}`,'DELETE');}
    catch(error){setNotice((error as Error).message);}finally{setBusy(false);}
  }

  const live=markets.filter(m=>m.books.length&&m.books.every(b=>b.status==='live')).length;
  const trackedGroups=[...new Map((state?.tracked??[]).map(m=>[eventKey(m),m])).values()];
  const plotStart=history[0]?.ts??0,plotSpan=Math.max((history.at(-1)?.ts??0)-plotStart,1);
  const plotLine=(key:string)=>{
    let drawing=false;
    return history.map(p=>{
      const value=p.values[key];if(value==null){drawing=false;return '';}
      const command=drawing?'L':'M';drawing=true;
      return `${command}${44+(p.ts-plotStart)/plotSpan*536},${176-value/10000*160}`;
    }).join(' ');
  };
  const resultMap=new Map<string,Market>();
  for(const m of results)if(!resultMap.has(marketKey(m))||m.outcome==='yes')resultMap.set(marketKey(m),m);
  const resultGroups=[...resultMap.values()];
  return <main>
    <header><a className="brand" href="/">PM<span>/</span><span className="brand-sub">MARKET DATA</span></a>
      <div className="header-right"><span className="pill">{state?.mode==='kafka'?'KAFKA':'LOCAL'} PIPELINE</span><span className={`connection ${connected?'online':''}`}><i/>{connected?'CONNECTED':'CONNECTING'}</span></div>
    </header>
    <div className="page-title"><div><p className="eyebrow">PUBLIC MARKET FEEDS</p><h1>See the book. Trust the data.</h1><p className="muted">Live depth, source events, and price gaps in one view.</p></div><button className="primary" onClick={()=>{setShowSearch(!showSearch);setNotice('');}}>+ Track a market</button></div>
    {state?.demo&&<div className="demo-banner"><b>SYNTHETIC DEMO</b><span>Sample prices test the pipeline. They are not live venue quotes. Add a real market to start its feed.</span></div>}
    {notice&&<div className="notice" role="status">{notice}<button onClick={()=>setNotice('')} aria-label="Dismiss">×</button></div>}
    {showSearch&&<section className="search-panel"><div className="search-line"><input autoFocus value={query} onChange={e=>setQuery(e.target.value)} onKeyDown={e=>e.key==='Enter'&&search()} placeholder="Search a topic or paste a Polymarket / Kalshi link" aria-label="Search markets"/><button className="primary" disabled={busy||!query.trim()} onClick={search}>{busy?'Working…':'Find markets'}</button><button onClick={()=>setShowSearch(false)} aria-label="Close search">×</button></div>
      {!!resultGroups.length&&<><div className="results-actions"><span>{resultGroups.length} open choices</span><button disabled={busy||!chosen.size} onClick={()=>track(results.filter(m=>chosen.has(marketKey(m))))}>Track selected ({chosen.size})</button><button disabled={busy} onClick={()=>track(results)}>Track all</button></div><div className="results">{resultGroups.map(m=><div className="result" key={marketKey(m)}><input type="checkbox" aria-label={`Select ${m.market_label??m.title}`} checked={chosen.has(marketKey(m))} onChange={()=>setChosen(previous=>{const n=new Set(previous);const id=marketKey(m);n.has(id)?n.delete(id):n.add(id);return n;})}/><div><b>{m.market_label??m.title}</b><small>{m.event_title??m.title}</small><small>{venueName(m.venue)} · {m.market_type&&m.market_type!=='markets'?`${typeLabel(m.market_type)} · `:''}{m.outcome_labels?.yes??'YES'} price {m.price??'N/A'} · 24h volume {m.volume_24h}</small></div><button disabled={busy} onClick={()=>track(results.filter(x=>marketKey(x)===marketKey(m)))}>Track</button></div>)}</div></>}
    </section>}
    <section className="stats"><div><small>MARKETS STREAMING</small><b>{live}<span> / {markets.length}</span></b></div><div><small>SAVED EVENTS</small><b>{trackedGroups.length}</b></div><div><small>RECENT GAP ALERTS</small><b>{state?.alerts.length??0}</b></div><div><small>BOOK ENGINE</small><b className="engine-label">C++20<span> exact integers</span></b></div></section>
    <section className="workspace">
      <aside><div className="section-heading"><h2>Events</h2><span>{events.length}</span></div><div className="book-list">{events.map(e=>{const m=e.markets[0],y=outcomeBook(m,'yes'),n=outcomeBook(m,'no'),status=e.markets.every(x=>x.books.length&&x.books.every(b=>b.status==='live'))?'live':'waiting for data';return <button key={e.key} onClick={()=>{setSelected(e.key);setSelectedChoice(m.key);}} className={`market ${event?.key===e.key?'selected':''}`}><div><span className="venue">{venueName(e.venue)}</span><span className={`status ${status==='live'?'live':''}`}>{status}</span></div><b>{e.title}</b>{m.books[0]?.data_origin==='synthetic'&&<small>SAMPLE</small>}{e.markets.length>1?<><small>{e.markets.length} choices</small><div className="event-preview">{[...e.markets].sort((a,b)=>(midpoint(outcomeBook(b,'yes'))??-1)-(midpoint(outcomeBook(a,'yes'))??-1)).slice(0,3).map(x=><div key={x.key}><span>{choiceLabel(x)}</span><strong>{probability(midpoint(outcomeBook(x,'yes')))}</strong></div>)}</div></>:<div className="market-outcomes"><span className="yes-text"><small>{sideLabel(m,'yes')}</small><strong>{probability(midpoint(y))}</strong></span><span className="no-text"><small>{sideLabel(m,'no')}</small><strong>{probability(midpoint(n))}</strong></span></div>}</button>;})}
        {!events.length&&<div className="empty"><p>Your first book starts here.</p><small>Track an open market by keyword or link.</small></div>}</div>
        {!!trackedGroups.length&&<div className="tracked"><p className="eyebrow">SAVED EVENTS</p>{trackedGroups.map(m=>{const count=new Set((state?.tracked??[]).filter(x=>eventKey(x)===eventKey(m)).map(marketKey)).size;return <div key={eventKey(m)}><span>{m.event_title??m.title}<small>{venueName(m.venue)} · {count} {count===1?'choice':'choices'}</small></span><button disabled={busy} onClick={()=>untrack((state?.tracked??[]).filter(x=>eventKey(x)===eventKey(m)))} aria-label={`Untrack ${m.event_title??m.title}`}>×</button></div>;})}</div>}
        <div className="access"><span className={state?.kalshi_ready?'good':''}>Kalshi {state?.kalshi_ready?'key available':'key required for live feed'}</span><small>Market data only. No order placement.</small></div>
      </aside>
      <article className="book-detail">{event&&market?<><div className="detail-title"><div><p className="eyebrow">{venueName(event.venue)} · {event.markets.length>1?`${event.markets.length} choices`:`${sideLabel(market,'yes')} / ${sideLabel(market,'no')}`}</p><h2>{event.title}</h2></div>{event.source_url&&<a href={event.source_url} target="_blank" rel="noreferrer">Open event ↗</a>}</div>
      {types.length>1&&<div className="market-types" role="group" aria-label="Market types">{types.map(type=><button key={type} aria-label={`View ${typeLabel(type)} markets`} aria-pressed={marketType(market)===type} onClick={()=>setSelectedChoice(event.markets.find(m=>marketType(m)===type)!.key)}>{typeLabel(type)} <small>{event.markets.filter(m=>marketType(m)===type).length}</small></button>)}</div>}
      {multiple&&<div className="event-choices" role="group" aria-label="Event choices">{choices.map((m,i)=><button key={m.key} title={m.meta.title} aria-label={`View ${choiceLabel(m)} book`} aria-pressed={market.key===m.key} onClick={()=>setSelectedChoice(m.key)} style={{borderColor:market.key===m.key?choiceColor(i):undefined}}><span><i style={{background:choiceColor(i)}}/><span>{choiceLabel(m)}{sideLabel(m,'yes').toLowerCase()!=='yes'&&<small>{sideLabel(m,'yes')}</small>}</span></span><strong>{probability(midpoint(outcomeBook(m,'yes')))}</strong></button>)}</div>}
      {labeledChoice&&<p className="choice-heading">Selected choice: <b>{choiceLabel(market)}</b></p>}
      {labeledChoice&&market.meta.title!==choiceLabel(market)&&<p className="choice-description">{market.meta.title}</p>}
      <div className="paired-quotes">{([yes,no] as const).map(b=>b&&<div key={b.outcome} className={`outcome-quote ${b.outcome}-text`}><div><span>{sideLabel(market,b.outcome)}</span><span className={`status ${b.status==='live'?'live':''}`}>{b.status.replaceAll('_',' ')}</span></div><strong>{probability(midpoint(b))}</strong><small>Bid {price(b.bids[0]?.price_e4)} · Ask {price(b.asks[0]?.price_e4)}</small>{b.derived&&<small>Calculated from {sideLabel(market,b.outcome==='no'?'yes':'no')} quotes</small>}</div>)}</div>
      <div className="spark"><div><small>{multiple?'CHOICE':'PAIRED'} MIDPOINTS · CURRENT SESSION</small><span>{history.length} updates</span></div><div className="chart-legend">{series.map(s=><span key={s.key} style={{color:s.color}}><i/>{s.label}</span>)}</div><svg viewBox="0 0 600 210" role="img" aria-label={multiple?'Event choice midpoint prices on the same time and probability scale':'Paired midpoint prices on the same time and probability scale'}>{[100,50,0].map(p=><g key={p}><line className={p===50?'midline':''} x1="44" x2="580" y1={176-p*1.6} y2={176-p*1.6}/><text x="0" y={180-p*1.6}>{p}%</text></g>)}{series.map(s=><path key={s.key} className={`${s.key}-line`} style={{stroke:s.color,strokeWidth:multiple&&s.key===market.key?3:2,opacity:multiple&&s.key!==market.key?.65:1}} d={plotLine(s.key)}/>)}<text x="44" y="202">Earlier</text><text x="550" y="202">Now</text></svg>{history.length<2&&<small className="muted">Waiting for the next price update.</small>}</div>
      <div className="depth-heading"><h3>Order book depth</h3><div className="outcome-switch" role="group" aria-label="Depth outcome">{(['yes','no'] as const).map(outcome=><button key={outcome} aria-pressed={depthOutcome===outcome} className={depthOutcome===outcome?`${outcome}-text active`:''} onClick={()=>setDepthOutcome(outcome)}>{sideLabel(market,outcome)}</button>)}</div></div><p className="depth-note">{labeledChoice?`${choiceLabel(market)} · `:''}{sideLabel(market,depthOutcome)} prices · Top 10 levels{book?.derived?' · Calculated from opposite quotes':''}</p>{!book&&<p className="muted">Waiting for a snapshot of this choice.</p>}<div className="depth">{(['bids','asks'] as const).map(side=>{const levels=book?.[side]??[];const largest=Math.max(...levels.map(l=>l.size_e6),1);return <div key={side} className={side}><div className="depth-label"><span>{side==='bids'?'BID PRICE':'ASK PRICE'}</span><span>CONTRACTS</span></div>{levels.map(l=><div className="level" key={l.price_e4}><div className="bar" style={{width:`${l.size_e6/largest*100}%`}}/><span>{price(l.price_e4)}</span><span>{quantity(l.size_e6)}</span></div>)}</div>;})}</div>
      <div className="integrity">{([yes,no] as const).map(b=>b&&<span key={b.outcome}>{sideLabel(market,b.outcome)} · {b.continuity} · Version {b.version}</span>)}</div><details className="lineage"><summary>Source events</summary>{([yes,no] as const).map(b=>b&&<code key={b.outcome}>{sideLabel(market,b.outcome)}{b.derived?' (calculated)':''}: {b.event_id}</code>)}<p>Public price levels. Individual order queue positions are not supplied.</p></details></>:<div className="detail-empty"><h2>Waiting for a market snapshot</h2><p>Use Track a market to connect a public order book.</p></div>}</article>
    </section>
    <section className="alerts"><div className="section-heading"><h2>Gap history</h2><span>Displayed quotes after the configured fee allowance</span></div>{state?.alerts.length?<div className="table-scroll"><table><thead><tr><th>PAIR</th><th>ASK / BID</th><th>GAP</th><th>TIME</th><th>SOURCE EVENTS</th></tr></thead><tbody>{[...state.alerts].reverse().slice(0,12).map(a=><tr key={a.alert_id}><td>{a.pair}<small>{a.data_origin.includes('synthetic')?'SYNTHETIC':'LIVE'} · {a.continuity.join(' / ')}</small></td><td className="mono">{price(a.ask_e4)} / {price(a.bid_e4)}</td><td className="good mono">{(a.gap_e4/100).toFixed(2)}%</td><td>{new Date(a.recv_ts_ns/1e6).toLocaleTimeString()}</td><td><details><summary>View lineage</summary>{a.source_event_ids.map(id=><code key={id}>{id}</code>)}</details></td></tr>)}</tbody></table></div>:<p className="muted">Alerts appear when a configured pair has fresh books and meets its gap threshold.</p>}</section>
    {!!state?.violations.length&&<details className="dq"><summary>Recent data quality events ({state.violations.length})</summary>{state.violations.slice(-10).reverse().map((v,i)=><p key={i}><b>{v.reason}</b><code>{v.event_id}</code></p>)}</details>}
    <footer><span>PM / DATA PLATFORM</span><span>Public quotes · Repeatable replay · No fill or profit claims</span></footer>
  </main>;
}
createRoot(document.getElementById('root')!).render(<App/>);
