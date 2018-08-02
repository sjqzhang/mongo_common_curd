package org.common.service;

import java.util.ArrayList;
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
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.util.JSON;

@Configuration
@Controller
@EnableAutoConfiguration
public class ApiController {
	
	
	private MongoClient mongoClient =null;

	@Autowired
	private Environment env;

	private String host = "127.0.0.1";

	String db = "test";

	String username = "";

	String password = "";

	String authdb = "";

	String port = "27017";
	
	String limit="1000";
	
	Long LIMIT=1000L;
	

	void init() {
		
		host=env.getProperty("host");
		db=env.getProperty("db");
		username=env.getProperty("username");
		password=env.getProperty("password");
		limit=env.getProperty("limit");
		
		if(mongoClient==null) {
			int p=27017;
			try {
				p=Integer.parseInt(port);
			} catch (Exception e) {
				
			}
			try {
				LIMIT=Long.parseLong(limit);
			} catch (Exception e) {
				
			}
			mongoClient= new MongoClient(host, p);
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
		
		public Response(String message,String status) {
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

	

	@RequestMapping("/cli/addobjs")
	@ResponseBody
	String addobjs(String table, String key, String data,HttpServletResponse resp) throws ParseException {
		init();
		setHeader(resp);
		String message = "";
		Object obj=null;
		boolean isList = false;
		try {

			 obj = JSON.parse(data);

			if (List.class.isInstance(obj)) {
				isList = true;
			}

		} catch (Exception e) {
			message = e.getMessage();
			System.out.println(e);
		}

		if (table == null) {

			message = "table is null";

		}
		
		MongoDatabase dbs = mongoClient.getDatabase(this.db);
		MongoCollection<Document> collection = dbs.getCollection(table);

		if (message != "") {
			return new Response(message).toJson();
		}

		if (key != null && key != "") {
			
			ObjectId id=new ObjectId(key);
			
		
			
			Document document=new Document();
			Map<String,Object> o=(Map<String,Object>)obj;
			for (String k : o.keySet()) {
				document.put(k, o.get(k));
			}
		
			
			UpdateResult result = collection.updateOne(new BasicDBObject("_id", new ObjectId(key)), new Document("$set", document));
			if (result.getModifiedCount()>0) {
				return new Response("ok").toJson();
			} else {
				return new Response("fail","fail").toJson();
			}

		} else {

			
			
			if(isList) {
			
				List list=(List)obj;
				List documents=new ArrayList<Document>();
	
				for(int i = 0 ; i < list.size() ; i++) {
					Map<String,Object> o=(Map<String,Object>)list.get(i);
					Document document = new Document();
					for (String k : o.keySet()) {
						document.put(k, o.get(k));
					}
					documents.add(document);
					
				}
				collection.insertMany(documents);
				return new Response("ok").toJson();
				
				
			} else {
				Document document=new Document();
				Map<String,Object> o=(Map<String,Object>)obj;
				for (String k : o.keySet()) {
					document.put(k, o.get(k));
				}
				collection.insertOne(document);
				return new Response("ok").toJson();
				
			}

		}

	}

	@RequestMapping("/cli/getobjs")
	@ResponseBody
	String getobjs(String sql,HttpServletResponse resp) throws ParseException {
		init();
		setHeader(resp);
		MongoDatabase db = mongoClient.getDatabase(this.db);
		QueryConverter queryConverter = new QueryConverter(sql);
		long limit = queryConverter.getMongoQuery().getLimit();
		if(limit<0){
			queryConverter.getMongoQuery().setLimit(LIMIT);
		}
		Object result = queryConverter.run(db);
		ArrayList documents = new ArrayList();
		if (QueryResultIterator.class.isInstance(result)) {
			QueryResultIterator<Document> iterator = (QueryResultIterator) result;
			while (iterator.hasNext()) {
				documents.add(iterator.next().toJson());
			}
		}
		queryConverter.getMongoQuery().setCountAll(true);
		Object cnt = queryConverter.run(db);
		HashMap<String, Object> map=new HashMap<String, Object>();
		map.put("count", cnt);
		map.put("rows", documents);
;		return new Response(map).toJson();
	}
	
	void setHeader(HttpServletResponse resp) {
//		resp.addHeader("Access-Control-Allow-Origin", "*");
		resp.setHeader("Content-Type", "application/json;charset=UTF-8");
	}
	
	@RequestMapping("/cli/getobjnames")
	@ResponseBody
	String getobjsname(HttpServletResponse resp) throws ParseException {
		init();
		setHeader(resp);
		MongoDatabase db = mongoClient.getDatabase(this.db);
		ListCollectionsIterable<Document> listColls = db.listCollections();

		return new Response(listColls).toJson();
	}

	public static void main(String[] args) throws Exception {

		SpringApplication.run(ApiController.class, args);
	}

}
