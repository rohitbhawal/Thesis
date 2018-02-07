/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.mrql;

import java_cup.runtime.*;
import org.apache.mrql.gen.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import java.util.Iterator;      //rohitb.n
import java.util.List;          //rohitb.n


/** Evaluates physical plans using one of the evaluation engines */
abstract public class Evaluator extends Interpreter {

    /** the current MRQL evaluator */
    public static Evaluator evaluator;

    /** initialize the evaluator */
    abstract public void init ( Configuration conf );

    /** shutdown the evaluator */
    abstract public void shutdown ( Configuration conf );

    /** initialize the query evaluation */
    abstract public void initialize_query ();

    /** create a new evaluation configuration */
    abstract public Configuration new_configuration ();

    /** synchronize peers in BSP mode */
    public MR_bool synchronize ( MR_string peerName, MR_bool mr_exit ) {
        throw new Error("You can only synchronize BSP tasks");
    }

    /** distribute a bag among peers in BSP mode */
    public Bag distribute ( MR_string peerName, Bag s ) {
        throw new Error("You can only distribute bags among BSP tasks");
    }

    /** run a BSP task */
    public MRData bsp ( Tree plan, Environment env ) throws Exception {
        throw new Error("You can only run a BSP task in BSP mode");
    }

    /** return the FileInputFormat for parsed files (CSV, XML, JSON, etc) */
    abstract public Class<? extends MRQLFileInputFormat> parsedInputFormat ();

    /** return the FileInputFormat for binary files */
    abstract public Class<? extends MRQLFileInputFormat> binaryInputFormat ();

    /** return the FileInputFormat for data generator files */
    abstract public Class<? extends MRQLFileInputFormat> generatorInputFormat ();

    /** Coerce a persistent collection to a Bag */
    abstract public Bag toBag ( MRData data );

    /** The Aggregate physical operator
     * @param acc_fnc  the accumulator function from (T,T) to T
     * @param zero  the zero element of type T
     * @param plan the plan that constructs the dataset that contains the bag of values {T}
     * @param env contains bindings fro variables to values (MRData)
     * @return the aggregation result of type T
     */
    abstract public MRData aggregate ( Tree acc_fnc,
                                       Tree zero,
                                       Tree plan,
                                       Environment env ) throws Exception;

    /** Evaluate a loop a fixed number of times */
    abstract public Tuple loop ( Tree e, Environment env ) throws Exception;

    /** Evaluate a MRQL physical plan and print tracing info
     * @param e the physical plan
     * @param env contains bindings fro variables to values (MRData)
     * @return a DataSet (stored in HDFS)
     */
    abstract public DataSet eval ( final Tree e,
                                   final Environment env,
                                   final String counter );

    final static MR_long counter_key = new MR_long(0);
    final static MRContainer counter_container = new MRContainer(counter_key);
    final static MRContainer value_container = new MRContainer(new MR_int(0));

    /** dump MRQL data into a sequence file */
    public void dump ( String file, Tree type, MRData data ) throws Exception {
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(Plan.conf);
        PrintStream ftp = new PrintStream(fs.create(path.suffix(".type")));
        ftp.print("2@"+type.toString()+"\n");
        ftp.close();
        SequenceFile.Writer writer
            = new SequenceFile.Writer(fs,Plan.conf,path,
                                      MRContainer.class,MRContainer.class);
        if (data instanceof MR_dataset)
            data = Plan.collect(((MR_dataset)data).dataset());
        if (data instanceof Bag) {
            Bag s = (Bag)data;
            long i = 0;
            for ( MRData e: s ) {
                counter_key.set(i++);
                value_container.set(e);
                writer.append(counter_container,value_container);
            }
        } else {
            counter_key.set(0);
            value_container.set(data);
            writer.append(counter_container,value_container);
        };
        writer.close();
    }

    /** dump MRQL data into a text CVS file */
    public void dump_text ( String file, Tree type, MRData data ) throws Exception {
	int ps = Config.max_bag_size_print;
	Config.max_bag_size_print = -1;
        if (!Config.hadoop_mode) {
            File parent = new File(file).getParentFile();
            if (parent != null && !parent.exists())
                parent.mkdirs();
        };
	final PrintStream out = (Config.hadoop_mode)
	                         ? Plan.print_stream(file)
	                         : new PrintStream(file);
	if (data instanceof MR_dataset)
	    data = Plan.collect(((MR_dataset)data).dataset());
	if (Translator.collection_type(type)) {
	    Tree tp = ((Node)type).children().head();
	    if (tp instanceof Node && ((Node)tp).name().equals("tuple")) {
		Trees ts = ((Node)tp).children();
		for ( MRData x: (Bag)data ) {
		    Tuple t = (Tuple)x;
		    out.print(print(t.get((short)0),ts.nth(0)));
		    for ( short i = 1; i < t.size(); i++ )
			out.print(","+print(t.get(i),ts.nth(i)));
		    out.println();
		}
	    } else for ( MRData x: (Bag)data )
		       out.println(print(x,tp));
	} else out.println(print(data,query_type));
	Config.max_bag_size_print = ps;
	out.close();
    }
    
    //rohitb.sn
    /** dumpjson MRQL data into a text CVS file */
    public void dump_json ( String file, Tree type, MRData data ) throws Exception {
    int ps = Config.max_bag_size_print;
	Config.max_bag_size_print = -1;
        if (!Config.hadoop_mode) {
            File parent = new File(file).getParentFile();
            if (parent != null && !parent.exists())
                parent.mkdirs();
        };
	final PrintStream out = (Config.hadoop_mode)
	                         ? Plan.print_stream(file)
	                         : new PrintStream(file);
	   
	 Boolean firstTime = Boolean.TRUE;                        
//     for ( MRData x: (Bag)data ){
//		if (!firstTime){
//			out.print(",");
//		}
//    	out.print(print_json(data, type, 0));
	 print_json(data, type, 0, out);
//    	firstTime = Boolean.FALSE;
//	}    
 
//	out.print("{");
//	if (data instanceof MR_dataset)
//	    data = Plan.collect(((MR_dataset)data).dataset());
//	if (Translator.collection_type(type)) {
//	    Tree tp = ((Node)type).children().head();
//	    if (tp instanceof Node && ((Node)tp).name().equals("tuple")) {
//		Trees ts = ((Node)tp).children();
//		for ( MRData x: (Bag)data ) {
//		    Tuple t = (Tuple)x;
////		    out.print("[");
//		    out.print(print_json(t.get((short)0),ts.nth(0)));
//		    for ( short i = 1; i < t.size(); i++ )
//			out.print(","+print_json(t.get(i),ts.nth(i)));
////		    out.println();
////		    out.print("]");
//		}
//	    } else {
//	    	
//	    	Boolean firstTime = Boolean.TRUE;
//	    	for ( MRData x: (Bag)data ){
////	    		System.out.println("JSON DUMP: "+x);
//	    		if (!firstTime){
//	    			out.print(",");
//	    		}
//		    	out.print(print_json(x, tp));  
//		    	firstTime = Boolean.FALSE;
//	    	}
////            out.print("]");
//	    }
//	} else {
//		out.println(print_json(data,query_type));
//	}
//	out.print("}");
	Config.max_bag_size_print = ps;
	out.close();
    }
    
    private void print_json ( MRData x, Tree type, int attrCount, PrintStream out ) {
    	try {
    		if (x instanceof Inv){
                //return print_json(((Inv)x).value(),type, attrCount, out);
    			print_json(((Inv)x).value(),type, attrCount, out);
    			return;
    		}
            if (type.equals(new VariableLeaf("XML"))){
                printtype_XML((Union)x, out);
                return;
            }
            if (type.equals(new VariableLeaf("JSON"))){
                printtype_JSON((Union)x, out);
                return;
            }
		
    		boolean FOUND_1 = false; 
    		Tree E_1 =  TypeInference.expand(type) ; 	

    		if ((E_1 instanceof Node) && ((Node)E_1).name==Tree.add("persistent") && ((Node)E_1).children.tail!=null && ((Node)E_1).children.tail.tail==null) 
    		{ 
    			Tree tp = ((Node)E_1).children.head;  FOUND_1=true; 
    			//return print_json(x,tp, attrCount, out);
    			print_json(x,tp, attrCount, out);
    			return;
    		}	
    		
    		if (!FOUND_1) 
    		{
    			if ((E_1 instanceof Node) && ((Node)E_1).name==Tree.add("Bag") && ((Node)E_1).children.tail!=null && ((Node)E_1).children.tail.tail==null) 
    				{ 
    				Tree tp = ((Node)E_1).children.head;  
    				FOUND_1=true; 
    				if (x instanceof MR_dataset) {
    					DataSet ds = ((MR_dataset)x).dataset();
    					List<MRData> vals = ds.take(Config.max_bag_size_print);
    					if (vals.size() == 0){
    						//return "[]";
    						out.print("[]");
    						return;
    					}
    					//String s = "[ "+print_json(vals.get(0),tp, attrCount);
    					out.print("[ ");
    					print_json(vals.get(0),tp, attrCount, out);
    					for ( int i = 1; i < vals.size(); i++ ){
    						//s += ", "+print_json(vals.get(i),tp, attrCount);
    						out.print(", ");
    						print_json(vals.get(i),tp, attrCount, out);
    					}
    					
    					//return s+" ]";
    					out.print(" ]");
    					return;
    				} 
    				else{
    					//return print_json(Evaluator.evaluator.toBag(x),new Node("bag",Trees.nil.append(Meta.escape(tp))), attrCount, out);
    					print_json(Evaluator.evaluator.toBag(x),new Node("bag",Trees.nil.append(Meta.escape(tp))), attrCount, out);
    					return;
    				}
    			}
    		}
    		
    		if (!FOUND_1) 
    		{
    			if ((E_1 instanceof Node) && ((Node)E_1).name==Tree.add("List") && ((Node)E_1).children.tail!=null && ((Node)E_1).children.tail.tail==null) 
    			{ 
    				Tree tp = ((Node)E_1).children.head;  
    				FOUND_1=true; 
    				if (x instanceof MR_dataset) {
    					DataSet ds = ((MR_dataset)x).dataset();
    					List<MRData> vals = ds.take(Config.max_bag_size_print);
    					if (vals.size() == 0){
    						//return "[]";
    						out.print("[]");
							return;
    					}
    					//String s = "[ "+print_json(vals.get(0),tp, attrCount);
    					out.print("[ ");
    					print_json(vals.get(0),tp, attrCount, out);
    					for ( int i = 1; i < vals.size(); i++ ){
    						//s += ", "+print_json(vals.get(i),tp, attrCount);
    						out.print(", ");
    						print_json(vals.get(i),tp, attrCount, out);
    					}
    					//return s+" ]";
    					out.print(" ]");
    					return;
    				} 
    				else{
    					//return print_json(Evaluator.evaluator.toBag(x),new Node("list",Trees.nil.append(Meta.escape(tp))), attrCount, out);
    					print_json(Evaluator.evaluator.toBag(x),new Node("list",Trees.nil.append(Meta.escape(tp))), attrCount, out);
    					return;
    				}
    			}
    			 
    		}
    		
    		if (!FOUND_1) 
    		{
    			if ((E_1 instanceof Node) && ((Node)E_1).name==Tree.add("bag") && ((Node)E_1).children.tail!=null && ((Node)E_1).children.tail.tail==null) 
    			{ 
    				Tree tp = ((Node)E_1).children.head;  
    				FOUND_1=true; 
    				Bag b = (Bag)x;
    				Iterator<MRData> bi = b.iterator();
    				if (!bi.hasNext()){
    					//return "[]";
    					out.print("[]");
    					return;
    				}
    				//String s = "[ "+print_json(bi.next(),tp, attrCount);
    				out.print("[ ");
    				print_json(bi.next(),tp, attrCount, out);
    				for ( long i = 1; bi.hasNext() ; i++ ){
    					//s += ", "+print_json(bi.next(),tp, attrCount);
    					out.print(", ");
    					print_json(bi.next(),tp, attrCount, out);
    				}
    				//return s+" ]";
    				out.print(" ]");
    				return;
    			}
    		}
    		
    		if (!FOUND_1) 
    		{
    			if ((E_1 instanceof Node) && ((Node)E_1).name==Tree.add("list") && ((Node)E_1).children.tail!=null && ((Node)E_1).children.tail.tail==null) 
    			{ 
    				Tree tp = ((Node)E_1).children.head;  
    				FOUND_1=true; 
    				Bag b = (Bag)x;
    				Iterator<MRData> bi = b.iterator();
    				if (!bi.hasNext()){
    					//return "[]";
    					out.print("[]");
    					return;
    				}
    				//String s = "[ "+print_json(bi.next(),tp, attrCount);
    				out.print("[ ");
    				print_json(bi.next(),tp, attrCount, out);
    				for ( long i = 1; bi.hasNext(); i++ ){
    					//s += ", "+print_json(bi.next(),tp, attrCount);
    					out.print(", ");
    					print_json(bi.next(),tp, attrCount, out);
    				}
    				//return s+" ]";
    				out.print(" ]");
    				return;
    			}
    		}
    		
    		if (!FOUND_1) 
    		{
    			if ((E_1 instanceof Node) && ((Node)E_1).name==Tree.add("tuple")) 
    			{ 
    				Trees el = ((Node)E_1).children;  
    				FOUND_1=true; 
    				Tuple t = (Tuple)x;
    				if (t.size() == 0){
    					//return "{}";
    					out.print("{}");
    					return;
    				}
    				//String s = "{"+quotedString("~"+String.valueOf(attrCount))+":"+print_json(t.get((short)0),el.nth(0), attrCount);
    				out.print("{"+quotedString("~"+String.valueOf(attrCount))+":");
    				print_json(t.get((short)0),el.nth(0), attrCount, out);
    				for ( short i = 1; i < t.size(); i++ ){
    					attrCount++;
    					//s += ","+quotedString("~"+String.valueOf(attrCount))+":"+print_json(t.get(i),el.nth(i), attrCount);
    					out.print(","+quotedString("~"+String.valueOf(attrCount))+":");
    					print_json(t.get(i),el.nth(i), attrCount, out);
    				}
    				//return s+"}";
    				out.print("}");
    				return;
    			}
    		}	
    		
    		if (!FOUND_1) 
    		{
    			if ((E_1 instanceof Node) && ((Node)E_1).name==Tree.add("record")) 
    			{ 
    				Trees el = ((Node)E_1).children;  FOUND_1=true; 
    				Tuple t = (Tuple)x;
    				if (t.size() == 0){
    					//return "{}";
    					out.print("{}");
    					return;
    				}
    				//String s = "{";
    				out.print("{");
    				 
    				boolean FOUND_2 = false; 
    				Tree E_2 =  el.nth(0) ; 
    				 
    				if ((E_2 instanceof Node) && ((Node)E_2).name==Tree.add("bind") && ((Node)E_2).children.tail!=null && ((Node)E_2).children.tail.tail!=null && ((Node)E_2).children.tail.tail.tail==null) 
    				{ 
    					Tree a = ((Node)E_2).children.head; 
    					Tree tp = ((Node)E_2).children.tail.head;  
    					FOUND_2=true; 
    					//s += quotedString(a.toString()) +": "+print_json(t.get((short)0),tp,attrCount);
    					out.print(quotedString(a.toString()) +": ");
    					print_json(t.get((short)0),tp,attrCount,out);
    				} 
    				
    				for ( short i = 1; i < t.size(); i++ )
    				{ 
    					boolean FOUND_3 = false; 
    					Tree E_3 =  el.nth(i) ; 
    					  
    					if ((E_3 instanceof Node) && ((Node)E_3).name==Tree.add("bind") && ((Node)E_3).children.tail!=null && ((Node)E_3).children.tail.tail!=null && ((Node)E_3).children.tail.tail.tail==null) 
    					{
    						Tree a = ((Node)E_3).children.head; 
    						Tree tp = ((Node)E_3).children.tail.head;  
    						FOUND_3=true; 
    						//s += ", "+quotedString(a.toString())+": "+print_json(t.get(i),tp,attrCount);
    						out.print(", "+quotedString(a.toString())+": ");
    						print_json(t.get(i),tp,attrCount,out);
    					}
    				}	
    					
    				//return s+"}";
    				out.print("}");
    				return;
    			}
    		}
    		
    		if (!FOUND_1) 
    		{
    			if ((E_1 instanceof Node) && ((Node)E_1).name==Tree.add("union")) 
    			{ 
    				Trees el = ((Node)E_1).children;  FOUND_1=true; 
    				Union u = (Union)x;
    				
    				boolean FOUND_2 = false; 
    				Tree E_2 =  el.nth(u.tag()) ; 
    			 
    				if ((E_2 instanceof Node) && ((Node)E_2).children.tail!=null && (((Node)E_2).children.head instanceof Node) && ((Node)((Node)E_2).children.head).name==Tree.add("tuple") && ((Node)E_2).children.tail.tail==null) 
    				{ 
    					String c = ((Node)E_2).name; 
    					Trees ts = ((Node)((Node)E_2).children.head).children;  
    					FOUND_2=true; 
    					//return c+print_json(u.value(),new Node("tuple",Trees.nil.append(ts)),attrCount);
    					out.print(c);
    					print_json(u.value(),new Node("tuple",Trees.nil.append(ts)),attrCount,out);
    					return;
    				}
    				
    				
    				if (!FOUND_2) 
    				{ 
    					if ((E_2 instanceof Node) && ((Node)E_2).children.tail!=null && ((Node)E_2).children.tail.tail==null) 
    					{ 
    					String c = ((Node)E_2).name; 
    					Tree tp = ((Node)E_2).children.head;  
    					FOUND_2=true; 
    					//return c+"{"+print_json(u.value(),tp,attrCount)+"}";
    					out.print(c+"{");
    					print_json(u.value(),tp,attrCount,out);
    					out.print("}");
    					return;
    					}
    					
    				}
    			}
    		}
    		
            String final_result = "";
    		try{
                double temp = Double.valueOf(x.toString());
                final_result = x.toString();
            }
            catch (NumberFormatException nfExp){
            	final_result = x.toString().replace("\"","");
                final_result = quotedString(final_result);
            }
                
    		//return final_result;
    		out.print(final_result);
    		return;
    	} 
    	catch (Exception ex) {
            throw new Error(ex);
        }
    }
    
	private String quotedString(String input){
		return "\"" + input + "\""; 
	}

	private static String quotedStaticString(String input){
		return "\"" + input + "\""; 
	}

    private static void printtype_XML ( final Union x,  PrintStream out) {
        if (x.tag() == 1){
            //return ((MR_string)x.value()).get();
        	out.print(((MR_string)x.value()).get());
        	return;
        }
        Tuple t = (Tuple)x.value();
        //String s = "<"+((MR_string)t.get(0)).get();
        out.print("<"+((MR_string)t.get(0)).get());
        for ( MRData a: (Bag)t.get(1) ) {
            Tuple attr = (Tuple)a;
//            s += " "+((MR_string)attr.first()).get()+"=\""
//                 +((MR_string)attr.second()).get()+"\"";
            out.print(" "+((MR_string)attr.first()).get()+"=\""
                         +((MR_string)attr.second()).get()+"\"");
        };
        Bag c = (Bag)t.get(2);
        if (c.size() == 0){
            //return s+"/>";
        	out.print("/>");
        	return;
        }
        //s += ">";
        out.print(">");
        for ( MRData e: c ){
            //s += printtype_XML((Union)e);
        	printtype_XML((Union)e, out);
        }
        //return s+"</"+((MR_string)t.get(0)).get()+">";
        out.print("</"+((MR_string)t.get(0)).get()+">");
        return;
    }

    private static void printtype_JSON ( final Union x, PrintStream out) {
    	Boolean first = Boolean.TRUE;
    	switch (x.tag()) {
        case 0:
        	first = Boolean.TRUE;
            //String s = "{ ";
        	out.print("{ ");
            for ( MRData e: (Bag)x.value() ) {
            	if (!first){
            		out.print(", ");
            		first = Boolean.FALSE;
            	}
                Tuple t = (Tuple)e;
                //s += t.get(0)+": "+printtype_JSON((Union)t.get(1))+", ";
                out.print(t.get(0)+": ");
                printtype_JSON((Union)t.get(1), out);
            };
            //return s.substring(0,s.length()-2)+" }";
            out.print(" }");
            return;
        case 1:
        	first = Boolean.TRUE;
            //String q = "[ ";
        	out.print("[ ");
            for ( MRData e: (Bag)x.value() ){
            	if (!first){
            		out.print(", ");
            		first = Boolean.FALSE;
            	}
                //q += printtype_JSON((Union)e)+", ";
            	printtype_JSON((Union)e, out);
            }
            //return q.substring(0,q.length()-2)+" ]";
            out.print(" ]");
            return;
        };
        //return ""+x.value();
        out.print(""+x.value());
        return;
    }
    
    private String print_json_in_memory ( MRData x, Tree type, int attrCount ) {
    	try {
    		if (x instanceof Inv)
                return print_json_in_memory(((Inv)x).value(),type, attrCount);
            if (type.equals(new VariableLeaf("XML")))
                return printtype_XML_in_memory((Union)x);
            if (type.equals(new VariableLeaf("JSON")))
                return printtype_JSON_in_memory((Union)x);
		
    		boolean FOUND_1 = false; 
    		Tree E_1 =  TypeInference.expand(type) ; 	

    		if ((E_1 instanceof Node) && ((Node)E_1).name==Tree.add("persistent") && ((Node)E_1).children.tail!=null && ((Node)E_1).children.tail.tail==null) 
    		{ 
    			Tree tp = ((Node)E_1).children.head;  FOUND_1=true; 
    			return print_json_in_memory(x,tp, attrCount);
    		}	
    		
    		if (!FOUND_1) 
    		{
    			if ((E_1 instanceof Node) && ((Node)E_1).name==Tree.add("Bag") && ((Node)E_1).children.tail!=null && ((Node)E_1).children.tail.tail==null) 
    				{ 
    				Tree tp = ((Node)E_1).children.head;  
    				FOUND_1=true; 
    				if (x instanceof MR_dataset) {
    					DataSet ds = ((MR_dataset)x).dataset();
    					List<MRData> vals = ds.take(Config.max_bag_size_print);
    					if (vals.size() == 0)
    						return "[]";
    					String s = "[ "+print_json_in_memory(vals.get(0),tp, attrCount);
    					for ( int i = 1; i < vals.size(); i++ )
    						s += ", "+print_json_in_memory(vals.get(i),tp, attrCount);
    					
    					return s+" ]";
    				} 
    			
    				else return print_json_in_memory(Evaluator.evaluator.toBag(x),new Node("bag",Trees.nil.append(Meta.escape(tp))), attrCount);
    			}
    		}
    		
    		if (!FOUND_1) 
    		{
    			if ((E_1 instanceof Node) && ((Node)E_1).name==Tree.add("List") && ((Node)E_1).children.tail!=null && ((Node)E_1).children.tail.tail==null) 
    			{ 
    				Tree tp = ((Node)E_1).children.head;  
    				FOUND_1=true; 
    				if (x instanceof MR_dataset) {
    					DataSet ds = ((MR_dataset)x).dataset();
    					List<MRData> vals = ds.take(Config.max_bag_size_print);
    					if (vals.size() == 0)
    						return "[]";
    					String s = "[ "+print_json_in_memory(vals.get(0),tp, attrCount);
    					for ( int i = 1; i < vals.size(); i++ )
    						s += ", "+print_json_in_memory(vals.get(i),tp, attrCount);
    					
    					return s+" ]";
    				} 
    				else return print_json_in_memory(Evaluator.evaluator.toBag(x),new Node("list",Trees.nil.append(Meta.escape(tp))), attrCount);
    			}
    			 
    		}
    		
    		if (!FOUND_1) 
    		{
    			if ((E_1 instanceof Node) && ((Node)E_1).name==Tree.add("bag") && ((Node)E_1).children.tail!=null && ((Node)E_1).children.tail.tail==null) 
    			{ 
    				Tree tp = ((Node)E_1).children.head;  
    				FOUND_1=true; 
    				Bag b = (Bag)x;
    				Iterator<MRData> bi = b.iterator();
    				if (!bi.hasNext())
    					return "[]";
    				String s = "[ "+print_json_in_memory(bi.next(),tp, attrCount);
    				for ( long i = 1; bi.hasNext() ; i++ )
    					s += ", "+print_json_in_memory(bi.next(),tp, attrCount);
    				return s+" ]";
    			}
    		}
    		
    		if (!FOUND_1) 
    		{
    			if ((E_1 instanceof Node) && ((Node)E_1).name==Tree.add("list") && ((Node)E_1).children.tail!=null && ((Node)E_1).children.tail.tail==null) 
    			{ 
    				Tree tp = ((Node)E_1).children.head;  
    				FOUND_1=true; 
    				Bag b = (Bag)x;
    				Iterator<MRData> bi = b.iterator();
    				if (!bi.hasNext())
    					return "[]";
    				String s = "[ "+print_json_in_memory(bi.next(),tp, attrCount);
    				for ( long i = 1; bi.hasNext(); i++ )
    					s += ", "+print_json_in_memory(bi.next(),tp, attrCount);
    				return s+" ]";
    			}
    		}
    		
    		if (!FOUND_1) 
    		{
    			if ((E_1 instanceof Node) && ((Node)E_1).name==Tree.add("tuple")) 
    			{ 
    				Trees el = ((Node)E_1).children;  
    				FOUND_1=true; 
    				Tuple t = (Tuple)x;
    				if (t.size() == 0)
    					return "{}";
    				String s = "{"+quotedString("~"+String.valueOf(attrCount))+":"+print_json_in_memory(t.get((short)0),el.nth(0), attrCount);
    				for ( short i = 1; i < t.size(); i++ ){
    					attrCount++;
    					s += ","+quotedString("~"+String.valueOf(attrCount))+":"+print_json_in_memory(t.get(i),el.nth(i), attrCount);
    				}
    				return s+"}";
    			}
    		}	
    		
    		if (!FOUND_1) 
    		{
    			if ((E_1 instanceof Node) && ((Node)E_1).name==Tree.add("record")) 
    			{ 
    				Trees el = ((Node)E_1).children;  FOUND_1=true; 
    				Tuple t = (Tuple)x;
    				if (t.size() == 0)
    					return "{}";
    				String s = "{";
    				 
    				boolean FOUND_2 = false; 
    				Tree E_2 =  el.nth(0) ; 
    				 
    				if ((E_2 instanceof Node) && ((Node)E_2).name==Tree.add("bind") && ((Node)E_2).children.tail!=null && ((Node)E_2).children.tail.tail!=null && ((Node)E_2).children.tail.tail.tail==null) 
    				{ 
    					Tree a = ((Node)E_2).children.head; 
    					Tree tp = ((Node)E_2).children.tail.head;  
    					FOUND_2=true; 
    					s += quotedString(a.toString()) +": "+print_json_in_memory(t.get((short)0),tp,attrCount);
    				} 
    				
    				for ( short i = 1; i < t.size(); i++ )
    				{ 
    					boolean FOUND_3 = false; 
    					Tree E_3 =  el.nth(i) ; 
    					  
    					if ((E_3 instanceof Node) && ((Node)E_3).name==Tree.add("bind") && ((Node)E_3).children.tail!=null && ((Node)E_3).children.tail.tail!=null && ((Node)E_3).children.tail.tail.tail==null) 
    					{
    						Tree a = ((Node)E_3).children.head; 
    						Tree tp = ((Node)E_3).children.tail.head;  
    						FOUND_3=true; 
    						s += ", "+quotedString(a.toString())+": "+print_json_in_memory(t.get(i),tp,attrCount);
    					}
    				}	
    					
    				return s+"}";
    			}
    		}
    		
    		if (!FOUND_1) 
    		{
    			if ((E_1 instanceof Node) && ((Node)E_1).name==Tree.add("union")) 
    			{ 
    				Trees el = ((Node)E_1).children;  FOUND_1=true; 
    				Union u = (Union)x;
    				
    				boolean FOUND_2 = false; 
    				Tree E_2 =  el.nth(u.tag()) ; 
    			 
    				if ((E_2 instanceof Node) && ((Node)E_2).children.tail!=null && (((Node)E_2).children.head instanceof Node) && ((Node)((Node)E_2).children.head).name==Tree.add("tuple") && ((Node)E_2).children.tail.tail==null) 
    				{ 
    					String c = ((Node)E_2).name; 
    					Trees ts = ((Node)((Node)E_2).children.head).children;  
    					FOUND_2=true; 
    					return c+print_json_in_memory(u.value(),new Node("tuple",Trees.nil.append(ts)),attrCount);
    				}
    				
    				
    				if (!FOUND_2) 
    				{ 
    					if ((E_2 instanceof Node) && ((Node)E_2).children.tail!=null && ((Node)E_2).children.tail.tail==null) 
    					{ 
    					String c = ((Node)E_2).name; 
    					Tree tp = ((Node)E_2).children.head;  
    					FOUND_2=true; 
    					return c+"{"+print_json_in_memory(u.value(),tp,attrCount)+"}";
    					}
    					
    				}
    			}
    		}
    		
            String final_result = "";
    		try{
                double temp = Double.valueOf(x.toString());
                final_result = x.toString();
            }
            catch (NumberFormatException nfExp){
            	final_result = x.toString().replace("\"","");
                final_result = quotedString(final_result);
            }
                
    		return final_result;
    	
    	} 
    	catch (Exception ex) {
            throw new Error(ex);
        }
       }
    
    private static String printtype_XML_in_memory ( final Union x ) {
        if (x.tag() == 1)
            return ((MR_string)x.value()).get();
        Tuple t = (Tuple)x.value();
        String s = "<"+((MR_string)t.get(0)).get();
        for ( MRData a: (Bag)t.get(1) ) {
            Tuple attr = (Tuple)a;
            s += " "+((MR_string)attr.first()).get()+"=\""
                 +((MR_string)attr.second()).get()+"\"";
        };
        Bag c = (Bag)t.get(2);
        if (c.size() == 0)
            return s+"/>";
        s += ">";
        for ( MRData e: c )
            s += printtype_XML_in_memory((Union)e);
        return s+"</"+((MR_string)t.get(0)).get()+">";
    }

    private static String printtype_JSON_in_memory ( final Union x ) {
        switch (x.tag()) {
        case 0:
            String s = "{ ";
            for ( MRData e: (Bag)x.value() ) {
                Tuple t = (Tuple)e;
                s += t.get(0)+": "+printtype_JSON_in_memory((Union)t.get(1))+", ";
            };
            return s.substring(0,s.length()-2)+" }";
        case 1:
            String q = "[ ";
            for ( MRData e: (Bag)x.value() )
                q += printtype_JSON_in_memory((Union)e)+", ";
            return q.substring(0,q.length()-2)+" ]";
        };
        return ""+x.value();
    }
    
    
    //rohitb.en

    /** evaluate plan in stream mode: evaluate each batch of data and apply the function f */
    public void streaming ( Tree plan, Environment env, Function f ) {
        throw new Error("MRQL Streaming is not supported in this evaluation mode yet");
    }

    /** for dumped data to a file, return the MRQL type of the data */
    public Tree get_type ( String file ) {
        try {
            Path path = new Path(file);
            FileSystem fs = path.getFileSystem(Plan.conf);
            BufferedReader ftp = new BufferedReader(new InputStreamReader(fs.open(path.suffix(".type"))));
            String s[] = ftp.readLine().split("@");
            ftp.close();
            if (s.length != 2 )
                return null;
            if (!s[0].equals("2"))
                throw new Error("The binary file has been created in java mode and cannot be read in hadoop mode");
            return Tree.parse(s[1]);
        } catch (Exception e) {
            return null;
        }
    }
}
