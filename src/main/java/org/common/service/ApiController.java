package org.common.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import com.github.vincentrussell.query.mongodb.sql.converter.QueryConverter;
import com.github.vincentrussell.query.mongodb.sql.converter.QueryResultIterator;
import com.github.vincentrussell.query.mongodb.sql.converter.SQLCommandType;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.util.JSON;

@Configuration
@Controller
@EnableAutoConfiguration
public class ApiController {

	private final Logger logger = LoggerFactory.getLogger(ApiController.class);

	// private MongoClient mongoClient = new MongoClient("10.1.50.90",27017);;

	private MongoClient mongoClient = null;

	@Autowired
	private Environment env;

	private String host = "127.0.0.1";

	String db = "test";

	String username = "";

	String password = "";

	String authdb = "";

	String port = "27017";

	String limit = "1000";

	String tablePrefix = "it_,sys_,dba_,bu_,sec_,dev_,test";

	Long LIMIT = 1000L;

	boolean debug = false;

	void init() {

		if (mongoClient == null) {

			host = env.getProperty("mongo.host") == null ? host : env.getProperty("mongo.host");
			port = env.getProperty("mongo.port") == null ? port : env.getProperty("mongo.port");
			authdb = env.getProperty("mongo.authdb") == null ? authdb : env.getProperty("mongo.authdb");
			db = env.getProperty("mongo.db") == null ? db : env.getProperty("mongo.db");
			username = env.getProperty("mongo.username") == null ? username : env.getProperty("mongo.username");
			password = env.getProperty("mongo.password") == null ? password : env.getProperty("mongo.password");
			limit = env.getProperty("limit") == null ? limit : env.getProperty("limit");
			tablePrefix = env.getProperty("table.prefix") == null ? tablePrefix : env.getProperty("table.prefix");

			int p = 27017;
			try {
				p = Integer.parseInt(port);
			} catch (Exception e) {

			}
			try {
				LIMIT = Long.parseLong(limit);
			} catch (Exception e) {

			}
			if (username != "" && password != "") {
				mongoClient = new MongoClient(new ServerAddress(host, p),
						Arrays.asList(MongoCredential.createCredential(username, authdb, password.toCharArray())));
			} else {
				mongoClient = new MongoClient(host + ":" + port);
			}

		}

	}

	class Response {
		private HashMap<String, Object> map = new HashMap<String, Object>();
		private Object data = null;
		private String message;
		private String status = "ok";

		public Response(Object data) {
			this.data = data;
		}

		public Response(Object data, String message) {
			this.data = data;
			this.message = message;

		}

		public Response(String message) {
			this.message = message;
		}

		public Response(String message, String status) {
			this.message = message;
			this.status = status;
		}

		public Response(Object data, String message, String status) {
			this.data = data;
			this.message = message;
			this.status = status;
		}

		public String toJson() {
			map.put("data", data);
			map.put("message", message);
			map.put("status", status);
			return JSON.serialize(map);

		}
	}

	Object getJSON(String data) {
		Object obj = null;
		try {
			obj = JSON.parse(data);
		} catch (Exception e) {

		}
		return obj;
	}

	boolean checkTablePrefix(String tablename) {

		boolean flag = false;

		String[] prefixs = tablePrefix.split(",");
		if (prefixs.length == 0) {
			return true;
		}
		for (String prefix : prefixs) {
			if (tablename.toLowerCase().startsWith(prefix)) {
				flag = true;
				break;
			}
		}
		return flag;
	}

	// String getClientIp(HttpServletRequest req) {
	//
	// return req.getRemoteAddr();
	// }

	public static String getClientIp(HttpServletRequest request) {

		String ip = request.getHeader("X-real-ip");

		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip =  request.getHeader("x-forwarded-for");
		}

		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("Proxy-Client-IP");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("WL-Proxy-Client-IP");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getRemoteAddr();
		}

		return ip;

	}

	@RequestMapping("/cli/addobjs")
	@ResponseBody
	String addobjs(String table, String key, String data, String is_merge,String query, HttpServletRequest req,
			HttpServletResponse resp) throws ParseException {

		logger.info(String.format("ip:%s, table:%s key:%s data:%s is_merge:%s", getClientIp(req), table, key, data,
				is_merge));
		try {
			
			if(table==null||table==""){
				return new Response("parameter table is null","fail").toJson();
			}
			
			if(data==null||data==""){
				return new Response("parameter data is null","fail").toJson();
			}
			
			if (!checkTablePrefix(table)) {
				return new Response("table name prefix is invalid,must be in " + tablePrefix,"fail").toJson();
			}
			init();
			setHeader(resp);
			String message = "";
			if (is_merge == null || is_merge == "") {
				is_merge = "1";
			}
			Object obj = null;
			Map<String,Object> jquery=null;
			boolean isList = false;
			obj = getJSON(data);
			if (obj == null) {
				return new Response("data must be json", "fail").toJson();
			} else {
				if (List.class.isInstance(obj)) {
					isList = true;
				} else {
					@SuppressWarnings("unchecked")
					Map<String, Object> o = (Map<String, Object>) (obj);
					if (o.containsKey("_id")) {
						return new Response("_id is internal key,not support", "fail").toJson();
					}
				}
			}

			if (table == null) {

				message = "table is null";
				return new Response(message).toJson();

			}

			MongoDatabase db = mongoClient.getDatabase(this.db);

			final MongoCollection<Document> collection = db.getCollection(table);

			if (message != "") {
				return new Response(message).toJson();
			}
			
			if(query!=null) {
				
				
				query = query.trim();
				jquery= (Map<String, Object>) getJSON(query);
				Document filter = new Document();
				if (query.startsWith("{") && query.endsWith("}")) {
				
					Map<String, Object> o = (Map<String, Object>) JSON.parse(query);
					for (String k : o.keySet()) {
						if (k.equals("_id")) {
							filter.put("_id", new ObjectId(o.get(k).toString()));
						} else {
							filter.put(k, o.get(k));
						}
					}
				}
				
				
				final Document obj2=new Document();
				for (String k : ((Map<String, Object>)obj).keySet()) {
					obj2.put(k, ((Map<String, Object>)obj).get(k));
				}

				UpdateResult result = collection.updateMany(filter, new Document("$set", obj2));
				
				
				if(result.getMatchedCount()>0) {
					return new Response(result.getModifiedCount(),"ok","ok").toJson();
				} else {
					return new Response(0,"fail","fail").toJson();
				}
				
		
//				collection.find(doc).forEach(new Block<Document>() {
//					public void apply(Document item) {
//						
//						if(item.containsKey("_id")) {
//							collection.updateOne(Filters.eq("_id", item.getObjectId("_id")), obj2);
//						}
//					}
//				});
				
				
			}

			if (key != null && key != "") {

				ObjectId id = new ObjectId(key);

				Document doc = collection.find(Filters.eq("_id", id)).first();

				if (doc == null) {
					return new Response(String.format("_id '%s' no found", key), "fail").toJson();
				}

				Document document = new Document();
				if (isList) {

					throw new Exception("Array not support");

				} else {

					if (is_merge == "1") {
						for (String k : doc.keySet()) {
							document.put(k, doc.get(k));
						}
					}
					@SuppressWarnings("unchecked")
					Map<String, Object> o = (Map<String, Object>) obj;
					o.put("_id", id);
					for (String k : o.keySet()) {
						document.put(k, o.get(k));
					}
				}
				UpdateResult result = collection.updateOne(Filters.eq("_id", id), new Document("$set", document));
				if (result.getModifiedCount() > 0) {
					return new Response("ok", "ok").toJson();
				} else if (result.getMatchedCount() > 0 && result.getModifiedCount() == 0) {
					return new Response("matched but not update", "ok").toJson();
				} else {
					return new Response("fail", "fail").toJson();
				}

			} else {

				if (isList) {

					@SuppressWarnings("unchecked")
					List<Map<String, Object>> list = (List<Map<String, Object>>) obj;
					List<Document> documents = new ArrayList<Document>();

					for (int i = 0; i < list.size(); i++) {
						Map<String, Object> o = (Map<String, Object>) list.get(i);
						Document document = new Document();
						for (String k : o.keySet()) {
							document.put(k, o.get(k));
						}
						documents.add(document);

					}
					collection.insertMany(documents);
					if (documents.size() > 0 && ((Document) documents.get(0)).getObjectId("_id") != null) {
						for (int i = 0; i < documents.size(); i++) {
							Document doc = (Document) documents.get(i);
							doc.put("_id", doc.getObjectId("_id").toString());
						}
						return new Response(documents, "ok", "ok").toJson();
					} else {
						return new Response("fail", "fail").toJson();
					}

				} else {
					Document document = new Document();
					@SuppressWarnings("unchecked")
					Map<String, Object> o = (Map<String, Object>) obj;
					for (String k : o.keySet()) {
						document.put(k, o.get(k));
					}

					collection.insertOne(document);

					if (document.getObjectId("_id") != null) {
						document.put("_id", document.getObjectId("_id").toString());
						return new Response(document, "ok", "ok").toJson();
					} else {
						return new Response("fail", "fail").toJson();
					}

				}

			}
		} catch (Exception e) {
			return new Response("Exception " + e.getMessage(), "fail").toJson();
		}

	}
	
	@RequestMapping("/cli/getquery")
	@ResponseBody
	String getquery(String sql, String query, HttpServletRequest req, HttpServletResponse resp) throws ParseException {
		
	   
		 
		
		return new Response("").toJson();
	}

	@RequestMapping("/cli/getobjs")
	@ResponseBody
	String getobjs(String sql, String query, HttpServletRequest req, HttpServletResponse resp) throws ParseException {
		logger.info(String.format("ip:%s, sql:%s query:%s", getClientIp(req), sql, query));
		try {
			init();
			setHeader(resp);
			String message = "";
			MongoDatabase db = mongoClient.getDatabase(this.db);
			if (sql == null || sql == "") {
				message = "sql is null";
				return new Response(message).toJson();
			}
			sql = sql.toLowerCase();
			QueryConverter queryConverter = new QueryConverter(sql);
			if (queryConverter.getMongoQuery().getSqlCommandType()==SQLCommandType.DELETE) {
				return new Response("delete was denied","fail").toJson();
			}
			long limit = queryConverter.getMongoQuery().getLimit();
			if (limit < 0) {
				queryConverter.getMongoQuery().setLimit(LIMIT);
			}
			if (query != null && query != "") {
				query = query.trim();

				Document doc = new Document();
				if (query.startsWith("{") && query.endsWith("}")) {
				
					Map<String, Object> o = (Map<String, Object>) JSON.parse(query);
					for (String k : o.keySet()) {
						if (k.equals("_id")) {
							doc.put("_id", new ObjectId(o.get(k).toString()));
						} else {
							doc.put(k, o.get(k));
						}
					}
				}

				queryConverter.getMongoQuery().setQuery(doc);
			}
			Object result = queryConverter.run(db);
			ArrayList<Document> documents = new ArrayList<Document>();
			if (QueryResultIterator.class.isInstance(result)) {
				
				QueryResultIterator<Document> iterator = (QueryResultIterator) result;
				while (iterator.hasNext()) {
					Document doc = iterator.next();
					if(doc.containsKey("_id")) {
						doc.put("_id", doc.getObjectId("_id").toString());
					}
					documents.add(doc);
				}
			}

			queryConverter.getMongoQuery().setCountAll(true);
			Object cnt = queryConverter.run(db);
			HashMap<String, Object> map = new HashMap<String, Object>();
			map.put("count", cnt);
			map.put("rows", documents);
			;
			return new Response(map).toJson();

		} catch (Exception e) {
			return new Response("Exception:"+e.getMessage()).toJson();
		}
	}

	void setHeader(HttpServletResponse resp) {
		// resp.addHeader("Access-Control-Allow-Origin", "*");
		resp.addHeader("Content-Type", "application/json;charset=UTF-8");
	}

	@RequestMapping(value = "/cli/getobjnames", produces = "application/json;charset=UTF-8")
	@ResponseBody
	String getobjsname(HttpServletRequest req, HttpServletResponse resp) throws ParseException {
		try {

			init();
			setHeader(resp);
			MongoDatabase db = mongoClient.getDatabase(this.db);
			ListCollectionsIterable<Document> listColls = db.listCollections();

			return new Response(listColls).toJson();
		} catch (Exception e) {
			return new Response(e.getMessage()).toJson();
		}
	}

	public static void main(String[] args) throws Exception {

		SpringApplication.run(ApiController.class, args);
		
//		QueryConverter queryConverter=new QueryConverter("select borough, cuisine, count(*) from my_collection WHERE borough LIKE 'Queens%' GROUP BY borough, cuisine ORDER BY count(*) DESC;");
//		
//		Document query = queryConverter.getMongoQuery().getQuery();
//		
//		System.out.println(JSON.serialize(query));
	}

}
