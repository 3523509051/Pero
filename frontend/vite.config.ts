import {defineConfig} from 'vite';
export default defineConfig({server:{host:'127.0.0.1',proxy:{'/manage':{target:'http://127.0.0.1:8766',changeOrigin:true,configure(proxy){proxy.on('proxyReq',(req)=>{if(req.getHeader('origin'))req.setHeader('origin','http://127.0.0.1:8766')})}}}}});
