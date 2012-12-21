package org.novusis.lucene;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

public class Lucene {

	private static String LUCENE_INDEXES = "resource/lucene_indexes";
	private static String LUCENE_RESOURCES = "resource/luceneResources.txt";
	private static String LUCINE_FIELD = "LUCINE_FIELD";
	private static int LIMIT_SEARCH = 100;
	
	//Search texts
	private static String prefixSeachText = "schw";
	private static String wildcardSearchText = "epid";
	private static String querySearchText = "muscle";
	private static String fuzzySearchText = "miscle";

	private IndexSearcher searcher = null;
	public static void main(String[] args) throws IOException {
		Lucene lucene = new Lucene();
		lucene.initializeLuceneIndexes();
		lucene.initilializeIndexSearcher(LUCENE_INDEXES);
		lucene.prefixSearch(prefixSeachText);
		lucene.wildcardSearch(wildcardSearchText);
		lucene.querySearch(querySearchText);
		lucene.fuzzySearch(fuzzySearchText);
	}
	
	
	public void prefixSearch(String prefix){
		System.out.println("EXECUTED PREFIX SEARCH. LIMIT TO "+LIMIT_SEARCH+" RESULTS. SEARCH TEXT = "+prefix);
		Query query = new PrefixQuery(new Term(LUCINE_FIELD, prefix));
		this.executeSearch(query);
	}
	
	public void wildcardSearch(String wildcardSeachText){
		System.out.println("EXECUTED WILDCARD SEARCH. LIMIT TO "+LIMIT_SEARCH+" RESULTS. SEARCH TEXT = "+wildcardSeachText);
		Query query = new WildcardQuery(new Term(LUCINE_FIELD, wildcardSeachText+"*"));
		this.executeSearch(query);
	}
	
	public void querySearch(String queryString){
		System.out.println("EXECUTED QUERY SEARCH. LIMIT TO "+LIMIT_SEARCH+" RESULTS. SEARCH TEXT = "+queryString);
		try {
			QueryParser parser = new QueryParser(Version.LUCENE_36, LUCINE_FIELD ,new StandardAnalyzer(Version.LUCENE_36));
        	Query query = parser.parse(queryString);
        	executeSearch(query);
        } catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	public void fuzzySearch(String fuzzySearchText){
		System.out.println("EXECUTED FUZZY SEARCH. LIMIT TO "+LIMIT_SEARCH+" RESULTS. SEARCH TEXT = "+fuzzySearchText);
		Term term = new Term(LUCINE_FIELD, fuzzySearchText);
		FuzzyQuery fuzzyQuery = new FuzzyQuery(term, 0.2f);
		executeSearch(fuzzyQuery);
	}
	
	public List<String> executeSearch(Query query) {
		try{
			System.err.println("===***==="+searcher.explain(query, LIMIT_SEARCH));
			TopDocs results = searcher.search(query,LIMIT_SEARCH);
			searcher.close();
			ScoreDoc[] hits = results.scoreDocs;
	        List<String> searchResults = new ArrayList<String>();
	        int count = 1;
	        for (ScoreDoc hit : hits) {
	        	Document doc = searcher.doc(hit.doc);
	        	String res = doc.get(LUCINE_FIELD);
	            searchResults.add(doc.get(LUCINE_FIELD));
	            System.err.println(count+"  "+res+", "+hit.score);
	            count++;
	        }
	        Collections.sort(searchResults);
			return searchResults;
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}
	
	private void initializeLuceneIndexes(){
		try {
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
			File file  = new File(LUCENE_INDEXES);
			IndexWriter indexWriter = new IndexWriter(FSDirectory.open(file), config);
			indexWriter.deleteAll();
			BufferedReader br = new BufferedReader(new FileReader(new File(LUCENE_RESOURCES)));
			String line;
			Map<String, Boolean> inserted = new HashMap<String, Boolean>();
			while ((line = br.readLine()) != null) {
				if(!inserted.containsKey(line.toLowerCase())){
					Document doc = new Document();
				 	doc.add(new Field(LUCINE_FIELD, line.toLowerCase(), Field.Store.YES, Field.Index.ANALYZED));
				 	
				 	indexWriter.addDocument(doc);
				 	inserted.put(line.toLowerCase(), true);
				}
			}
			br.close();
			indexWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void initilializeIndexSearcher(String folderName)  {
		try {
			Directory directory = new SimpleFSDirectory(new File(folderName));
		    IndexReader indexReader=IndexReader.open(directory);
		    this.searcher = new IndexSearcher(indexReader);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
