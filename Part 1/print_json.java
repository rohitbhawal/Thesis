public String print_json ( MRData x, Tree type ) {
	try {
		boolean FOUND_1 = false; 
		Tree E_1 =  TypeInference.expand(type) ; 	

		if ((E_1 instanceof Node) && ((Node)E_1).name==Tree.add("persistent") && ((Node)E_1).children.tail!=null && ((Node)E_1).children.tail.tail==null) 
		{ 
			Tree tp = ((Node)E_1).children.head;  FOUND_1=true; 
			return print(x,tp);
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
						return "{}";
					String s = "{ "+print(vals.get(0),tp);
					for ( int i = 1; i < vals.size(); i++ )
						s += ", "+print(vals.get(i),tp);
					
					return s+" }";
				} 
			
				else return print(Evaluator.evaluator.toBag(x),new Node("bag",Trees.nil.append(Meta.escape(tp))));
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
					String s = "[ "+print(vals.get(0),tp);
					for ( int i = 1; i < vals.size(); i++ )
						s += ", "+print(vals.get(i),tp);
					
					return s+" ]";
				} 
				else return print(Evaluator.evaluator.toBag(x),new Node("list",Trees.nil.append(Meta.escape(tp))));
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
					return "{}";
				String s = "{ "+print(bi.next(),tp);
				for ( long i = 1; bi.hasNext() ; i++ )
					s += ", "+print(bi.next(),tp);
				return s+" }";
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
				String s = "[ "+print(bi.next(),tp);
				for ( long i = 1; bi.hasNext(); i++ )
					s += ", "+print(bi.next(),tp);
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
					return "()";
				String s = "("+print(t.get((short)0),el.nth(0));
				for ( short i = 1; i < t.size(); i++ )
					s += ","+print(t.get(i),el.nth(i));
				return s+")";
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
					s += a+": "+print(t.get((short)0),tp);
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
						s += ", "+a+": "+print(t.get(i),tp);
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
					return c+print(u.value(),new Node("tuple",Trees.nil.append(ts)));
				}
				
				
				if (!FOUND_2) 
				{ 
					if ((E_2 instanceof Node) && ((Node)E_2).children.tail!=null && ((Node)E_2).children.tail.tail==null) 
					{ 
					String c = ((Node)E_2).name; 
					Tree tp = ((Node)E_2).children.head;  
					FOUND_2=true; 
					return c+"{"+print(u.value(),tp)+"}";
					}
					
				}
			}
		}
		
		
		return x.toString();
	
	} 
	catch (Exception ex) {
        throw new Error(ex);
    }
   }