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
import one.nio.rpc.RpcServer;
import one.nio.server.ServerConfig;

import org.h2.jdbcx.JdbcConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * <p>
 * A h2 backend cache server.
 * </p>
 * 
 * @author little-pan
 * @since 2018-07-12
 */
public class H2CacheServer implements CacheService<Entity> {
	
    private final JdbcConnectionPool pool;
    {
    	System.setProperty("h2.mvStore", "false");
    	String url = "jdbc:h2:./data/cache;PAGE_SIZE=4096";
    	pool = JdbcConnectionPool.create(url, "sa", "");
    }

    private H2CacheServer() {
    	Connection conn = null;
        try {
        	conn = getConn();
        	Statement stmt = conn.createStatement();
        	stmt.executeUpdate("create table if not exists tbl_entity("
        			+ "id bigint not null primary key, "
        			+ "content other"
        			+ ")");
        	stmt.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			close(conn);
		}
    }
    
    private Connection getConn() throws SQLException {
    	return pool.getConnection();
    }

    @Override
    public Entity get(long key) throws SQLException {
    	Entity content = null;
    	Connection conn = null;
		try {
			conn = getConn();
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select id, content from tbl_entity where id="+key);
			if(rs.next()){
				content = (Entity)rs.getObject(2);
			}
			rs.close();
			stmt.close();
			return content;
		} finally {
			close(conn);
		}
    }

    @Override
    public void set(long key, Entity value) throws SQLException {
    	Connection conn = null;
    	try{
    		conn = getConn();
    		conn.setAutoCommit(false);
    		
    		Statement stmt = conn.createStatement();
    		final String query = String.format("select id, content from tbl_entity where id=%d for update", key);
    		ResultSet rs = stmt.executeQuery(query);
    		if(rs.next()){
    			final PreparedStatement updateStmt = 
    				conn.prepareStatement("update tbl_entity set content=? where id=?");
    			updateStmt.setObject(1, value);
    			updateStmt.setLong(2, key);
    			updateStmt.executeUpdate();
    			updateStmt.close();
    		}else{
    			final PreparedStatement insertStmt = 
    				conn.prepareStatement("insert into tbl_entity(id, content)values(?,?)");
    			insertStmt.setLong(1, key);
    			insertStmt.setObject(2, value);
    			insertStmt.executeUpdate();
    			insertStmt.close();
    		}
    		rs.close();
    		
    		conn.commit();
    	}catch(final SQLException e){
    		if(conn != null){
				conn.rollback();
			}
    	}finally{
    		close(conn);
    	}
    }
    
    private void close(AutoCloseable close){
    	if(close != null){
			try {
				close.close();
			} catch (Exception e) {}
		}
    }

    public static void main(String[] args) throws Exception {
    	final String yml = "acceptors:\n -port: "+DEFAULT_PORT;
    	ServerConfig config = ConfigParser.parse(yml, ServerConfig.class);
    	
        RpcServer<H2CacheServer> server = 
        	new RpcServer<H2CacheServer>(config, new H2CacheServer());
        server.start();
    }
}
