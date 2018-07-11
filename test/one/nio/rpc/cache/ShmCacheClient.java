/*
 * Copyright 2015 Odnoklassniki Ltd, Mail.Ru Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package one.nio.rpc.cache;

import one.nio.net.ConnectionString;
import one.nio.rpc.RpcClient;

import java.io.IOException;
import java.lang.reflect.Proxy;

public class ShmCacheClient {

    public static void main(String[] args) throws Exception {
    	final boolean debug = Boolean.getBoolean("debug");
    	
        if (args.length < 3) {
            showUsage();
        }

        ConnectionString conn = new ConnectionString(args[0] + ':' + CacheService.DEFAULT_PORT);
        final RpcClient client = new RpcClient(conn);
        CacheService<Entity> cacheClient = getCacheService(client);
        String cmd = args[1];
        long n = Long.parseLong(args[2]);

        final long ts = System.currentTimeMillis();
        if (args.length == 3 && cmd.equals("get")) {
        	long items = 0L;
        	for(long i = 0L; i < n; ++i){
        		Entity value = cacheClient.get(i);
        		if(value != null){
        			++items;
        		}
        		if(debug){
        			final String val = value==null?null:value.getData();
        			System.out.println(String.format("get(%d) = %s", i, val));
        		}
        	}
        	System.out.println(String.format("[GET] time = %dms, items = %d, ops = %d", 
        			(System.currentTimeMillis() - ts), items, n));
        } else if (args.length == 4 && cmd.equals("set")) {
        	final String pfx = args[3];
        	for(long i = 0L; i < n; ++i){
        		final Entity value = new Entity(i, pfx + i);
        		cacheClient.set(i, value);
                if(debug){
                	System.out.println(String.format("set(%d, %s)", i, value.getData()));
                }
        	}
        	System.out.println(String.format("[SET] time = %dms, ops = %d", 
        			(System.currentTimeMillis() - ts), n));
        } else {
            showUsage();
        }
        
        client.close();
    }

    private static void showUsage() {
        System.out.println("Usage: java " + ShmCacheClient.class.getName() + " <host> [get <n> | set <n> <prefix>]");
        System.exit(1);
    }

    @SuppressWarnings("unchecked")
    private static CacheService<Entity> getCacheService(RpcClient client) throws IOException {
        return (CacheService<Entity>) Proxy.newProxyInstance(
                CacheService.class.getClassLoader(), new Class[] { CacheService.class }, client);
    }
}
