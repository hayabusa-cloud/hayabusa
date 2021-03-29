# HAYABUSA Framework

Hayabusa is a server side framework for Japan-like social games.  
・Easy to understand and use for beginners  
・Powerful controller, flexible deployment  
・High performance, high throughput and concurrent

### Installation
go get github.com/hayabusa-cloud/hayabusa

### Quick start
https://github.com/hayabusa-cloud/hybs-quickstart

### Has modules or features as follows:

#### HTTP/1.1 and HTTP/3 via QUIC

・HTTP/1.1 on tcp4 or tcp6 socket  
・HTTP/3 on udp4 or udp6 socket

#### HTTP API controller

・REST-style API design   
・fast URL locating  
・builtin reverse proxy  
・builtin access controll   
・builtin basic authentication   
・builtin request parameter check     
・useful builtin middlewares library   
・etc.

#### API document generation
・auto generate basic API document

#### Realtime network communication via UDP socket

・low latency, low memory usage and high throughput  
・micro second level time cost per request  
・supporting KCP and QUIC protocol  
・server side application layer logic  
・no need to care concurrency safety in application layer   
&nbsp;&nbsp;(there are exceptions )  
・multi-play room management and builtin matching algorithm  
・builtin area of interest algorithm  
・builtin basic authentication  
・useful builtin middlewares library  

#### Plugins and modules

・logger and log collection  
・event management, scheduler, timer  
・csv master data management  
・local memory cache  
・redis connection management  
・distributed lock/mutex  
・mongodb connections management  
・mysql connections management  
・sqlite connections management   
・builtin basic authentication  

#### Coming Soon
・test cases and benchmark   
・sample game server side onestop solutionship   
&nbsp;&nbsp;(central server, platform server, game server, realtime server, admin website, batch)   
・csharp sdk and unity sample project for realtime communication   
・js sdk and cocos sample project for realtime communication   

#### Contact
E-mail: hayabusa-cloud@outlook.jp