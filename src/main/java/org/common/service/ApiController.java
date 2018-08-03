package org.common.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.bson.Document;
import org.bson.types.ObjectId;
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

	String tablePrefix = "";

	Long LIMIT = 1000L;

	void init() {

		host = env.getProperty("host");
		db = env.getProperty("db");
		username = env.getProperty("username");
		password = env.getProperty("password");
		limit = env.getProperty("limit");
		tablePrefix = env.getProperty("table.prefix");
		if (tablePrefix == null || tablePrefix == "") {
			tablePrefix = env.getProperty("tablePrefix");
		}

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
			if(username!=null && password!=null) {
				mongoClient = new MongoClient(new ServerAddress( host , p),	Arrays.asList(MongoCredential.createCredential(username,authdb,password.toCharArray())));
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

	@RequestMapping("/cli/addobjs")
	@ResponseBody
	String addobjs(String table, String key, String data, HttpServletResponse resp) throws ParseException {
		try {
			if (!checkTablePrefix(table)) {
				return new Response("tablename prefix is invalid,must be in " + tablePrefix).toJson();
			}
			init();
			setHeader(resp);
			String message = "";
			Object obj = null;
			boolean isList = false;
			try {

				obj = JSON.parse(data);

				if (List.class.isInstance(obj)) {
					isList = true;
				}

			} catch (Exception e) {
				message = e.getMessage();
				return new Response(message).toJson();
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
					return new Response("ok").toJson();
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
					return new Response("ok").toJson();

				} else {
					Document document = new Document();
					Map<String, Object> o = (Map<String, Object>) obj;
					for (String k : o.keySet()) {
						document.put(k, o.get(k));
					}
					// String sql = "select * from "+ table.toLowerCase()+
					// "limit 1";
					// QueryConverter queryConverter = new QueryConverter(sql);
					// queryConverter.getMongoQuery().setCountAll(true);
					// queryConverter.getMongoQuery().setQuery(document);
					// Object count = queryConverter.run(db);
					// if(count!=null && Long.parseLong(count.toString())>0){
					// collection.updateOne(document, document);
					// }

					collection.insertOne(document);
					return new Response("ok").toJson();

				}

			}
		} catch (Exception e) {
			return new Response(e.getMessage()).toJson();
		}

	}

	@RequestMapping("/cli/getobjs")
	@ResponseBody
	String getobjs(String sql, String query, HttpServletResponse resp) throws ParseException {

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
		resp.setHeader("Content-Type", "application/json;charset=UTF-8");
	}

	@RequestMapping("/cli/getobjnames")
	@ResponseBody
	String getobjsname(HttpServletResponse resp) throws ParseException {
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
