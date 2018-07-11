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

import one.nio.config.ConfigParser;
import one.nio.mem.SharedMemoryLongMap;
import one.nio.mem.SharedMemoryMap;
import one.nio.rpc.RpcServer;
import one.nio.server.ServerConfig;

import java.io.IOException;

public class ShmCacheServer implements CacheService<Entity> {
    private SharedMemoryMap<Long, Entity> cacheImpl;

    private ShmCacheServer() {
        try {
        	final int cap = 100 * (1<<20);
        	final String file = "data/cache.shm";
			this.cacheImpl = new SharedMemoryLongMap<Entity>(cap, file, 1<<30);
			this.cacheImpl.setSerializer(Entity.class);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    }

    @Override
    public Entity get(long key) {
        return cacheImpl.get(key);
    }

    @Override
    public void set(long key, Entity value) {
        cacheImpl.put(key, value);
    }

    public static void main(String[] args) throws Exception {
    	final String yml = "acceptors:\n -port: "+DEFAULT_PORT;
    	ServerConfig config = ConfigParser.parse(yml, ServerConfig.class);
    	
        RpcServer<ShmCacheServer> server = 
        	new RpcServer<ShmCacheServer>(config, new ShmCacheServer());
        server.start();
    }
}
