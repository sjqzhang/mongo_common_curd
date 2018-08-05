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
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
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

	String tablePrefix = "it_,sys_,dba_,bu_,sec_,dev_";

	Long LIMIT = 1000L;

	boolean debug = false;

	void init() {
		
		host = env.getProperty("mongo.host") == null ? host : env.getProperty("mongo.host");
		port = env.getProperty("mongo.port") == null ? port : env.getProperty("mongo.port");
		authdb = env.getProperty("mongo.authdb") == null ? authdb : env.getProperty("mongo.authdb");
		db = env.getProperty("mongo.db") == null ? db : env.getProperty("mongo.db");
		username = env.getProperty("mongo.username") == null ? username : env.getProperty("mongo.username");
		password = env.getProperty("mongo.password") == null ? password : env.getProperty("mongo.password");
		limit = env.getProperty("limit") == null ? limit : env.getProperty("limit");
		tablePrefix = env.getProperty("table.prefix") == null ? tablePrefix : env.getProperty("table.prefix");
 
//        System.out.println(host+ port+ authdb+db+username+password+limit);		

		if (mongoClient == null) {


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

	String getClientIp(HttpServletRequest req) {

		return req.getRemoteAddr();
	}

	@RequestMapping("/cli/addobjs")
	@ResponseBody
	String addobjs(String table, String key, String data, HttpServletRequest req, HttpServletResponse resp)
			throws ParseException {
		
		logger.info(String.format("ip:%s, table:%s key:%s data:%s", getClientIp(req), table, key, data));
		try {
			if (!checkTablePrefix(table)) {
				return new Response("table name prefix is invalid,must be in " + tablePrefix).toJson();
			}
			init();
			setHeader(resp);
			String message = "";
			Object obj = null;
			boolean isList = false;
			obj = getJSON(data);
			if (obj == null) {
				return new Response("data must be json", "fail").toJson();
			} else {
				if (List.class.isInstance(obj)) {
					isList = true;
				}
			}

			if (table == null) {

				message = "table is null";
				return new Response(message).toJson();

			}

			MongoDatabase db = mongoClient.getDatabase(this.db);

			MongoCollection<Document> collection = db.getCollection(table);

			if (message != "") {
				return new Response(message).toJson();
			}

			if (key != null && key != "") {

				ObjectId id = new ObjectId(key);
				Document document = new Document();
				Map<String, Object> o = (Map<String, Object>) obj;
				for (String k : o.keySet()) {
					document.put(k, o.get(k));
				}
				UpdateResult result = collection.updateOne(new BasicDBObject("_id", new ObjectId(key)),
						new Document("$set", document));
				if (result.getModifiedCount() > 0) {
					return new Response("ok", "ok").toJson();
				} else {
					return new Response("fail", "fail").toJson();
				}

			} else {

				if (isList) {

					List list = (List) obj;
					List documents = new ArrayList<Document>();

					for (int i = 0; i < list.size(); i++) {
						Map<String, Object> o = (Map<String, Object>) list.get(i);
						Document document = new Document();
						for (String k : o.keySet()) {
							document.put(k, o.get(k));
						}
						documents.add(document);

					}
					collection.insertMany(documents);
					if(documents.size()>0&&((Document)documents.get(0)).getObjectId("_id")!=null) {
						return new Response(documents, "ok", "ok").toJson();
					} else {
						return new Response("fail", "fail").toJson();
					}

				} else {
					Document document = new Document();
					Map<String, Object> o = (Map<String, Object>) obj;
					for (String k : o.keySet()) {
						document.put(k, o.get(k));
					}

					collection.insertOne(document);
					
					if(document.getObjectId("_id")!=null){
						return new Response(document, "ok","ok").toJson();	
					} else {
						return new Response("fail", "fail").toJson();
					}
				
					

				}

			}
		} catch (Exception e) {
			return new Response("Exception"+e.getMessage(), "fail").toJson();
		}

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
			String collectionName = queryConverter.getMongoQuery().getCollection();

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
			ArrayList documents = new ArrayList();
			if (QueryResultIterator.class.isInstance(result)) {
				QueryResultIterator<Document> iterator = (QueryResultIterator) result;
				while (iterator.hasNext()) {
					documents.add(iterator.next());
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
			return new Response(e.getMessage()).toJson();
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
	}

}
