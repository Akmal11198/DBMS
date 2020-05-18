import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

public class DBApp {
	static int nodeSize;
	static int N;

	public void serialize(Serializable s, String path) {
		try {
			FileOutputStream fileOut = new FileOutputStream(path + ".ser");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(s);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public Object deserialize(String path) throws ClassNotFoundException, IOException {

		FileInputStream fileIn = null;
		try {
			fileIn = new FileInputStream(path + ".ser");
		} catch (FileNotFoundException e) {
			System.out.println("file  not found");
		}

		ObjectInputStream in = new ObjectInputStream(fileIn);
		Serializable p = (Serializable) in.readObject();
		in.close();
		fileIn.close();

		return p;
	}

	public void init() throws IOException {
		try (PrintWriter writer = new PrintWriter("metadata.csv")) {
			StringBuilder sb = new StringBuilder();
			sb.append("Table Name, Column Name, Column Type, ClusteringKey, Indexed");
			sb.append('\n');
			writer.write(sb.toString());
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		}

		// reading configurations
		FileReader reader = new FileReader("DBApp.properties");
		Properties p = new Properties();
		p.load(reader);
		N = Integer.parseInt(p.getProperty("MaximumRowsCountinPage"));
		ArrayList<String> tablesNames = new ArrayList<String>();
		serialize(tablesNames, "tablesNames");

	}

	private boolean checkDataTypes(String type) {
		if ((type == "java.lang.Integer") || (type == "java.lang.String") || (type == "java.lang.Double")
				|| (type == " java.util.Date") || (type == "java.lang.Boolean") || (type == "java.awt.Polygon"))
			return true;
		return false;

	}

	@SuppressWarnings("resource")
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, ClassNotFoundException, IOException {
		String line;
		BufferedReader csvReader = new BufferedReader(new FileReader("metadata.csv"));
		boolean tableExists = false;
		while ((line = csvReader.readLine()) != null) {
			String[] row = line.split(",");
			if (row[0].equals(strTableName))
				tableExists = true;
		}
		if (tableExists) {
			System.out.println("exists");
			throw new DBAppException("A table with the same name already exists");

		}

		Table t = new Table();
		System.out.println(strTableName);
		String old = "";
		String row;
		csvReader = new BufferedReader(new FileReader("metadata.csv"));
		try {
			while ((row = csvReader.readLine()) != null)
				old += row + "\n";
			csvReader.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try (PrintWriter writer = new PrintWriter("metadata.csv")) {
			StringBuilder sb = new StringBuilder();
			sb.append(old);
			Enumeration<String> enu = htblColNameType.keys();
			while (enu.hasMoreElements()) {
				String key = enu.nextElement();
				if (!checkDataTypes(htblColNameType.get(key)))
					throw new DBAppException("Wrong data types entered");
				// indexing is missing here
				sb.append(strTableName + "," + key + "," + htblColNameType.get(key) + ","
						+ (key == strClusteringKeyColumn) + "," + "false" + ",");
				sb.append('\n');

			}
			writer.write(sb.toString());
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		}
		serialize(t, strTableName);
	}

	@SuppressWarnings({ "unchecked", "resource" })
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ClassNotFoundException {
		// boolean tablefound = false;
		boolean indexed = false;
		Table table = null;
		String line;
		BufferedReader csvReader = new BufferedReader(new FileReader("metadata.csv"));
		if (!checkTable(strTableName))
			throw new DBAppException("table does not exist");
		try {
			table = (Table) deserialize(strTableName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		boolean polygonClusteringKey = false;
		FileReader reader = new FileReader("DBApp.properties");
		Properties pr = new Properties();
		pr.load(reader);
		N = Integer.parseInt(pr.getProperty("MaximumRowsCountinPage"));
		nodeSize = Integer.parseInt(pr.getProperty("NodeSize"));
		Object primaryKey = null;
		int clusteringKeyIndex = -1;
		Vector<Object> record = new Vector<Object>();
		table.ColNames = new ArrayList<String>();
		csvReader = new BufferedReader(new FileReader("metadata.csv"));
		try {
			while ((line = csvReader.readLine()) != null) {
				String[] row = line.split(",");
				if (row[0].equals(strTableName)) {
					String key = row[1];
					table.ColNames.add(key);
					if (!htblColNameValue.containsKey(key))
						throw new DBAppException("Column unavailable in input");
					if (!(htblColNameValue.get(key).getClass().getCanonicalName()).equals(row[2])) {
						serialize(table, strTableName);
						throw new DBAppException("incompatible types");
					} else {
						if (row[2].equals("java.awt.Polygon")) {
							// Serializable g = (Polygon) htblColNameValue.get(row[1]);
							// g.serialVersionUID++;
							// g.
							record.add((Polygon) htblColNameValue.get(row[1]));
							if (row[3].equals("true"))
								polygonClusteringKey = true;
						} else {
							record.add(htblColNameValue.get(key));
						}
					}
					if (row[3].equals("true")) {
						primaryKey = row[1];
						clusteringKeyIndex = record.size() - 1;
						if (row[4].equals("true"))
							indexed = true;
					}
				}
			}
			DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
			LocalDateTime now = LocalDateTime.now();
			record.add(dtf.format(now));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		csvReader.close();
		int index = -1;
		// Case no pages were previously generated
		if (table.pageNames.size() == 0) {
			Page p = new Page(N);
			p.rows.add(record);
			p.min = record.get(clusteringKeyIndex);
			System.out.println(p.min);
			table.nPages++;
			table.pageNames.add(strTableName + table.nPages);
			// insert in all indices
			for (int i = 0; i < table.BPTNames.size(); i++) {
				BPTree b = (BPTree) deserialize(strTableName + table.BPTNames.get(i));
				Ref r = new Ref(table.pageNames.get(0), 0);
				b.insert((Comparable) record.get(table.ColNames.indexOf(table.BPTNames.get(i))), r);
				serialize(b, strTableName + table.BPTNames.get(i));
			}
			for (int i = 0; i < table.RTNames.size(); i++) {
				RTree b = (RTree) deserialize(strTableName + table.RTNames.get(i));
				Ref r = new Ref(table.pageNames.get(0), 0);
				DBpolygon pp = new DBpolygon((Polygon) record.get(table.ColNames.indexOf(table.RTNames.get(i))));
				b.insert(pp, r);
				serialize(b, strTableName + table.RTNames.get(i));
			}
			serialize(p, strTableName + table.nPages);
			serialize(table, strTableName);
			return;
		}
		// case there is at least a page

		// polygon
		if (polygonClusteringKey) {
			DBpolygon po = new DBpolygon((Polygon) htblColNameValue.get(primaryKey));
			// get the page
			Page p = null;
			String page = null;
			// if RTree exists on clustering key
			if (indexed) {
				RTree b = (RTree) deserialize(strTableName + primaryKey);

				page = b.getPage(po);
				serialize(b, strTableName + primaryKey);
				p = (Page) deserialize(page);
				if ((new DBpolygon((Polygon) p.min)).compareTo(po) > 0 && table.pageNames.indexOf(page) > 0) {
					serialize(p, page);
					int in = table.pageNames.indexOf(page);
					page = table.pageNames.get(in - 1);
					p = (Page) deserialize(page);
				}
			} else {
				int min = 0;
				int max = table.pageNames.size() - 1;
				int mid = -1;
				while (max >= min) {
					mid = (min + max) / 2;
					page = table.pageNames.get(mid);
					p = (Page) deserialize(page);
					if ((new DBpolygon((Polygon) p.min)).compareTo(po) > 0)
						max = mid - 1;
					else
						min = mid + 1;
				}
				if ((new DBpolygon((Polygon) p.min)).compareTo(po) > 0 && table.pageNames.indexOf(page) > 0) {
					System.out.println("insert in previous page");
					serialize(p, page);
					page = table.pageNames.get(table.pageNames.indexOf(page) - 1);
					p = (Page) deserialize(page);

				}
			}
			// get the index in the page
			System.out.println(table.pageNames.indexOf(page));
			System.out.println(po.getArea());
			int min = 0;
			int max = p.rows.size() - 1;
			int mid = -1;
			while (max >= min && min > -1 && max > -1) {
				mid = (min + max) / 2;
				System.out.println(min + " " + mid + " " + max);

				if ((new DBpolygon((Polygon) p.rows.get(mid).get(clusteringKeyIndex))).compareTo(po) > 0) {
					max = mid - 1;
					index = mid;
				} else
					min = mid + 1;
			}
			if ((new DBpolygon((Polygon) p.rows.get(p.rows.size() - 1).get(clusteringKeyIndex))).compareTo(po) <= 0)
				index = p.rows.size();

			if (index == N) {
				// check
				if (table.pageNames.indexOf(page) == table.pageNames.size() - 1) {
					Page p2 = new Page(N);
					p2.rows.add(record);
					p2.min = record.get(clusteringKeyIndex);
					table.nPages++;
					table.pageNames.add(strTableName + table.nPages);
					// insert in all indices
					for (int i = 0; i < table.BPTNames.size(); i++) {
						BPTree b = (BPTree) deserialize(strTableName + table.BPTNames.get(i));
						Ref r = new Ref(table.pageNames.get(table.pageNames.size() - 1), 0);
						b.insert((Comparable) record.get(table.ColNames.indexOf(table.BPTNames.get(i))), r);
						serialize(b, strTableName + table.BPTNames.get(i));
					}
					for (int i = 0; i < table.RTNames.size(); i++) {
						RTree b = (RTree) deserialize(strTableName + table.RTNames.get(i));
						Ref r = new Ref(table.pageNames.get(table.pageNames.size() - 1), 0);
						DBpolygon pp = new DBpolygon(
								(Polygon) record.get(table.ColNames.indexOf(table.RTNames.get(i))));
						b.insert(pp, r);
						serialize(b, strTableName + table.RTNames.get(i));
					}
					serialize(p2, strTableName + table.nPages);
					serialize(table, strTableName);
					System.out.println("N on last page");
					return;
				}
				index = 0;
				page = table.pageNames.get(table.pageNames.indexOf(page) + 1);
				p = (Page) deserialize(page);
			}
			// insertion
			Ref r = new Ref(page, index);
			Ref newr = new Ref(page, index);
			Ref oldr = null;
			Comparable key = null;
			Vector<Object> toInsert = record;
			Vector<Object> toInsert2 = null;
			while (true) {
				if (index == 0) {
					p.min = toInsert.get(clusteringKeyIndex);
				}
				for (int i = index; i < p.rows.size(); i++) {
					toInsert2 = p.rows.get(i);
					p.rows.set(i, toInsert);
					// updating reference
					if (!toInsert.equals(record)) {
						for (int j = 0; j < table.BPTNames.size(); j++) {
							BPTree b = (BPTree) deserialize(strTableName + table.BPTNames.get(j));
							key = (Comparable) toInsert.get(table.ColNames.indexOf(table.BPTNames.get(j)));
							newr = new Ref(page, i);
							b.updateRef(key, newr, oldr);
							serialize(b, strTableName + table.BPTNames.get(j));
						}
						for (int j = 0; j < table.RTNames.size(); j++) {
							RTree b = (RTree) deserialize(strTableName + table.RTNames.get(j));
							DBpolygon pp = new DBpolygon(
									(Polygon) toInsert.get(table.ColNames.indexOf(table.RTNames.get(j))));
							newr = new Ref(page, i);
							b.updateRef(pp, newr, oldr);
							serialize(b, strTableName + table.RTNames.get(j));
						}
					}
					// done
					System.out.println("inserted " + toInsert + " " + table.pageNames.indexOf(page));
					toInsert = toInsert2;
					oldr = new Ref(page, i);
				}
				if (p.rows.size() < N) {
					p.rows.add(toInsert);
					// updating reference
					newr = new Ref(page, p.rows.size() - 1);
					if (!toInsert.equals(record)) {
						for (int j = 0; j < table.BPTNames.size(); j++) {
							BPTree b = (BPTree) deserialize(strTableName + table.BPTNames.get(j));
							key = (Comparable) toInsert.get(table.ColNames.indexOf(table.BPTNames.get(j)));
							b.updateRef(key, newr, oldr);
							serialize(b, strTableName + table.BPTNames.get(j));
						}
						for (int j = 0; j < table.RTNames.size(); j++) {
							RTree b = (RTree) deserialize(strTableName + table.RTNames.get(j));
							DBpolygon pp = new DBpolygon(
									(Polygon) toInsert.get(table.ColNames.indexOf(table.RTNames.get(j))));
							b.updateRef(pp, newr, oldr);
							serialize(b, strTableName + table.RTNames.get(j));
						}
					}
					// done
					serialize(p, page);
					System.out.println("added " + toInsert);
					break;
				}
				serialize(p, page);
				if (table.pageNames.indexOf(page) == table.pageNames.size() - 1) {
					Page p2 = new Page(N);
					p2.rows.add(toInsert);
					p2.min = toInsert.get(clusteringKeyIndex);
					table.nPages++;
					table.pageNames.add(strTableName + table.nPages);
					page = strTableName + table.nPages;
					// updating reference
					if (!toInsert.equals(record)) {
						for (int j = 0; j < table.BPTNames.size(); j++) {
							BPTree b = (BPTree) deserialize(strTableName + table.BPTNames.get(j));
							key = (Comparable) toInsert.get(table.ColNames.indexOf(table.BPTNames.get(j)));
							newr = new Ref(page, 0);
							b.updateRef(key, newr, oldr);
							serialize(b, strTableName + table.BPTNames.get(j));
						}
						for (int j = 0; j < table.RTNames.size(); j++) {
							RTree b = (RTree) deserialize(strTableName + table.RTNames.get(j));
							newr = new Ref(page, 0);
							DBpolygon pp = new DBpolygon(
									(Polygon) toInsert.get(table.ColNames.indexOf(table.RTNames.get(j))));
							b.updateRef(pp, newr, oldr);
							serialize(b, strTableName + table.RTNames.get(j));
						}
					}
					// done
					serialize(p2, strTableName + table.nPages);
					System.out.println("inserted in new page " + page);
					break;
				}
				p = (Page) deserialize(table.pageNames.get(table.pageNames.indexOf(page) + 1));
				page = table.pageNames.get((table.pageNames.indexOf(page) + 1));
				index = 0;
				System.out.println("insert in next page");
			}
			// insert in all indices
			for (int i = 0; i < table.BPTNames.size(); i++) {
				BPTree b = (BPTree) deserialize(strTableName + table.BPTNames.get(i));
				b.insert((Comparable) record.get(table.ColNames.indexOf(table.BPTNames.get(i))), r);
				serialize(b, strTableName + table.BPTNames.get(i));
			}
			for (int i = 0; i < table.RTNames.size(); i++) {
				RTree b = (RTree) deserialize(strTableName + table.RTNames.get(i));
				DBpolygon pp = new DBpolygon((Polygon) record.get(table.ColNames.indexOf(table.RTNames.get(i))));
				b.insert(pp, r);
				serialize(b, strTableName + table.RTNames.get(i));
			}
			serialize(table, strTableName);
			return;
		}

		// not polygon
		Comparable<Object> value = (Comparable<Object>) htblColNameValue.get(primaryKey);
		// get the page
		Page p = null;
		String page = null;
		// if BPTree exists on clustering key
		if (indexed) {
			BPTree b = (BPTree) deserialize(strTableName + primaryKey);

			page = b.getPage((Comparable) record.get(clusteringKeyIndex));
			serialize(b, strTableName + primaryKey);
			p = (Page) deserialize(page);
			if (((Comparable<Object>) p.min).compareTo(value) > 0 && table.pageNames.indexOf(page) > 0) {
				serialize(p, page);
				int in = table.pageNames.indexOf(page);
				page = table.pageNames.get(in - 1);
				p = (Page) deserialize(page);
			}
		} else {
			int min = 0;
			int max = table.pageNames.size() - 1;
			int mid = -1;
			while (max >= min) {
				mid = (min + max) / 2;
				page = table.pageNames.get(mid);
				p = (Page) deserialize(page);
				if (((Comparable) p.min).compareTo(value) > 0)
					max = mid - 1;
				else
					min = mid + 1;
			}
			if (((Comparable<Object>) p.min).compareTo(value) > 0 && table.pageNames.indexOf(page) > 0) {
				System.out.println("insert in previous page");
				serialize(p, page);
				page = table.pageNames.get(table.pageNames.indexOf(page) - 1);
				p = (Page) deserialize(page);

			}
		}

		// get the index in the page
		System.out.println(table.pageNames.indexOf(page));
		System.out.println(value);
		int min = 0;
		int max = p.rows.size() - 1;
		int mid = -1;
		while (max >= min && min > -1 && max > -1) {
			mid = (min + max) / 2;
			System.out.println(min + " " + mid + " " + max);
			if (((Comparable) p.rows.get(mid).get(clusteringKeyIndex)).compareTo(value) > 0) {
				max = mid - 1;
				index = mid;
			} else
				min = mid + 1;
		}
		if (((Comparable<Object>) p.rows.get(p.rows.size() - 1).get(clusteringKeyIndex)).compareTo(value) <= 0)
			index = p.rows.size();

		if (index == N) {
			// check
			if (table.pageNames.indexOf(page) == table.pageNames.size() - 1) {
				Page p2 = new Page(N);
				p2.rows.add(record);
				p2.min = record.get(clusteringKeyIndex);
				table.nPages++;
				table.pageNames.add(strTableName + table.nPages);
				// insert in all indices
				for (int i = 0; i < table.BPTNames.size(); i++) {
					BPTree b = (BPTree) deserialize(strTableName + table.BPTNames.get(i));
					Ref r = new Ref(table.pageNames.get(table.pageNames.size() - 1), 0);
					b.insert((Comparable) record.get(table.ColNames.indexOf(table.BPTNames.get(i))), r);
					serialize(b, strTableName + table.BPTNames.get(i));
				}
				for (int i = 0; i < table.RTNames.size(); i++) {
					RTree b = (RTree) deserialize(strTableName + table.RTNames.get(i));
					Ref r = new Ref(table.pageNames.get(table.pageNames.size() - 1), 0);
					DBpolygon pp = new DBpolygon((Polygon) record.get(table.ColNames.indexOf(table.RTNames.get(i))));
					b.insert(pp, r);
					serialize(b, strTableName + table.RTNames.get(i));
				}
				serialize(p2, strTableName + table.nPages);
				serialize(table, strTableName);
				System.out.println("N on last page");
				return;
			}
			index = 0;
			page = table.pageNames.get(table.pageNames.indexOf(page) + 1);
			p = (Page) deserialize(page);
		}
		// insertion
		Ref r = new Ref(page, index);
		Ref newr = new Ref(page, index);
		Ref oldr = null;
		Comparable key = null;
		Vector<Object> toInsert = record;
		Vector<Object> toInsert2 = null;
		while (true) {
			if (index == 0) {
				p.min = toInsert.get(clusteringKeyIndex);
			}
			for (int i = index; i < p.rows.size(); i++) {
				toInsert2 = p.rows.get(i);
				p.rows.set(i, toInsert);
				// updating reference
				if (!toInsert.equals(record)) {
					for (int j = 0; j < table.BPTNames.size(); j++) {
						BPTree b = (BPTree) deserialize(strTableName + table.BPTNames.get(j));
						key = (Comparable) toInsert.get(table.ColNames.indexOf(table.BPTNames.get(j)));
						newr = new Ref(page, i);
						b.updateRef(key, newr, oldr);
						serialize(b, strTableName + table.BPTNames.get(j));
					}
					for (int j = 0; j < table.RTNames.size(); j++) {
						RTree b = (RTree) deserialize(strTableName + table.RTNames.get(j));
						DBpolygon pp = new DBpolygon(
								(Polygon) toInsert.get(table.ColNames.indexOf(table.RTNames.get(j))));
						newr = new Ref(page, i);
						b.updateRef(pp, newr, oldr);
						serialize(b, strTableName + table.RTNames.get(j));
					}
				}
				// done
				System.out.println("inserted " + toInsert + " " + table.pageNames.indexOf(page));
				toInsert = toInsert2;
				oldr = new Ref(page, i);
			}
			if (p.rows.size() < N) {
				p.rows.add(toInsert);
				// updating reference
				newr = new Ref(page, p.rows.size() - 1);
				key = (Comparable) toInsert.get(clusteringKeyIndex);
				if (!toInsert.equals(record)) {
					for (int j = 0; j < table.BPTNames.size(); j++) {
						BPTree b = (BPTree) deserialize(strTableName + table.BPTNames.get(j));
						key = (Comparable) toInsert.get(table.ColNames.indexOf(table.BPTNames.get(j)));
						b.updateRef(key, newr, oldr);
						serialize(b, strTableName + table.BPTNames.get(j));
					}
					for (int j = 0; j < table.RTNames.size(); j++) {
						RTree b = (RTree) deserialize(strTableName + table.RTNames.get(j));
						DBpolygon pp = new DBpolygon(
								(Polygon) toInsert.get(table.ColNames.indexOf(table.RTNames.get(j))));
						b.updateRef(pp, newr, oldr);
						serialize(b, strTableName + table.RTNames.get(j));
					}
				}
				// done
				serialize(p, page);
				System.out.println("added " + toInsert);
				break;
			}
			serialize(p, page);
			if (table.pageNames.indexOf(page) == table.pageNames.size() - 1) {
				Page p2 = new Page(N);
				p2.rows.add(toInsert);
				p2.min = toInsert.get(clusteringKeyIndex);
				table.nPages++;
				table.pageNames.add(strTableName + table.nPages);
				page = strTableName + table.nPages;
				// updating reference
				if (!toInsert.equals(record)) {
					for (int j = 0; j < table.BPTNames.size(); j++) {
						BPTree b = (BPTree) deserialize(strTableName + table.BPTNames.get(j));
						key = (Comparable) toInsert.get(table.ColNames.indexOf(table.BPTNames.get(j)));
						newr = new Ref(page, 0);
						b.updateRef(key, newr, oldr);
						serialize(b, strTableName + table.BPTNames.get(j));
					}
					for (int j = 0; j < table.RTNames.size(); j++) {
						RTree b = (RTree) deserialize(strTableName + table.RTNames.get(j));
						newr = new Ref(page, 0);
						DBpolygon pp = new DBpolygon(
								(Polygon) toInsert.get(table.ColNames.indexOf(table.RTNames.get(j))));
						b.updateRef(pp, newr, oldr);
						serialize(b, strTableName + table.RTNames.get(j));
					}
				}
				// done
				serialize(p2, strTableName + table.nPages);
				System.out.println("inserted in new page " + page);
				break;
			}
			p = (Page) deserialize(table.pageNames.get(table.pageNames.indexOf(page) + 1));
			page = table.pageNames.get((table.pageNames.indexOf(page) + 1));
			index = 0;
			System.out.println("insert in next page");
		}
		// insert in all indices
		for (int i = 0; i < table.BPTNames.size(); i++) {
			BPTree b = (BPTree) deserialize(strTableName + table.BPTNames.get(i));
			b.insert((Comparable) record.get(table.ColNames.indexOf(table.BPTNames.get(i))), r);
			serialize(b, strTableName + table.BPTNames.get(i));
		}
		for (int i = 0; i < table.RTNames.size(); i++) {
			RTree b = (RTree) deserialize(strTableName + table.RTNames.get(i));
			DBpolygon pp = new DBpolygon((Polygon) record.get(table.ColNames.indexOf(table.RTNames.get(i))));
			b.insert(pp, r);
			serialize(b, strTableName + table.RTNames.get(i));
		}
		serialize(table, strTableName);
	}

	@SuppressWarnings("unchecked")
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, ClassNotFoundException, IOException {
		boolean tablefound = checkTable(strTableName);
		boolean isPolygon = false;
		boolean isClusteringKey = false;
		String clusteringKey = "";
		int clusteringKeyIndex = -1;
		int index = -1;
		if (!tablefound)
			throw new DBAppException("table does not exist");
		Table t = (Table) deserialize(strTableName);
		String line;
		boolean indexed = false;
		String indexedColName = null;
		// check if indexed column
		BufferedReader csvReader = new BufferedReader(new FileReader("metadata.csv"));
		while ((line = csvReader.readLine()) != null) {
			String[] row = line.split(",");
			if (row[0].equals(strTableName)) {
				index++;
				if (row[4].equals("true") && htblColNameValue.containsKey(row[1])) {
					if (row[2].equals("java.awt.Polygon"))
						isPolygon = true;
					indexed = true;
					indexedColName = row[1];
					break;
				} else if (row[3].equals("true") && htblColNameValue.containsKey(row[1])) {
					clusteringKey = row[1];
					isClusteringKey = true;
					clusteringKeyIndex = index;
				}

			}
		}
		if (indexed) {
			System.out.println("Indexed");
			if (isPolygon)
				deleteRTree(htblColNameValue, strTableName, indexedColName, index);
			else
				deleteBPTree(htblColNameValue, strTableName, indexedColName, index);
			return;
		}

		boolean binaryFound = false;
		if (isClusteringKey) {
			System.out.println("binary search");
			Vector<String> pages = t.pageNames;
			int size = pages.size();
			int first = 1;
			Page p;
			while (size > first) {
				System.out.println(size);
				System.out.println(first);
				p = (Page) deserialize(pages.get(size / 2));

				if (((Comparable) htblColNameValue.get(clusteringKey)).compareTo(p.min) < 0) {
					size = size / 2;
					System.out.println("yes");
				}
				if (size == 1)
					break;
				else {
					first = (first + size + 1) / 2;
				}
			}
			
			first = first -1;

			int i;
			//System.out.println(first - 1);
			
			
			p = (Page) deserialize(pages.get(first));
			while(true) {
			for (i = 0; i < p.rows.size(); i++) {
				System.out.println(p.rows.size());
				Vector tuple = p.rows.get(i);
				boolean found = true;
				int in = -1;
				csvReader = new BufferedReader(new FileReader("metadata.csv"));
				try {
					while ((line = csvReader.readLine()) != null) {
						String[] row = line.split(",");
						if (row[0].equals(strTableName)) {
							in++;
							String key = row[1];
							if (htblColNameValue.get(key) != null) {
								if (row[2].equals("java.awt.Polygon")) {

									if ((new DBpolygon((Polygon) tuple.get(in)))
											.compareTo(new DBpolygon((Polygon) (htblColNameValue.get(key)))) != 0) {
										found = false;
										break;
									}
								}
								else if (!(tuple.get(in).equals(htblColNameValue.get(key)))) {
									System.out.println(tuple.get(in));
									System.out.println(htblColNameValue.get(key));
									System.out.println("flase");
									found = false;
									//break;
								}
							}
							if (row[3].equals("true")) {
								// primaryKey=row[1];
								// clusteringKeyIndex = in;
							}
						}
					}
				} catch (IOException e1) {
					e1.printStackTrace();
				}

				boolean pageExists = true;

				if (found) {
					System.out.println("found");
					if (p.rows.size() <= 1) {
						System.out.println("deleted page");
						p.rows.remove(i);
						t.pageNames.remove(first);
						i--;
						pageExists = false;
						//break;
					} else {
						System.out.println("removed " +i);
						p.rows.remove(i);
						i--;
						p.min = p.rows.get(0).get(clusteringKeyIndex);
					}

				}

				if (pageExists)
					serialize(p, t.pageNames.get(first));
				System.out.println("serialize");
				serialize(t, strTableName);
			}
			first++;
			if(first>=pages.size())
				break;
			System.out.println("test");
			p = (Page) deserialize(pages.get(first));
			if(((Comparable) htblColNameValue.get(clusteringKey)).compareTo(p.min) > 0)
				break;
			}
			return;
		}

		System.out.println("not indexed");

		FileReader reader = new FileReader("DBApp.properties");
		Properties pr = new Properties();
		pr.load(reader);
		N = Integer.parseInt(pr.getProperty("MaximumRowsCountinPage"));
		for (int i = 0; i < t.pageNames.size(); i++) {
			boolean pageExists = true;
			Page p = (Page) deserialize(t.pageNames.get(i));
			for (int j = 0; j < p.rows.size(); j++) {
				Vector<Object> tuple = p.rows.get(j);
				boolean found = true;
				int in = -1;
				csvReader = new BufferedReader(new FileReader("metadata.csv"));
				try {
					while ((line = csvReader.readLine()) != null) {
						String[] row = line.split(",");
						if (row[0].equals(strTableName)) {
							in++;
							String key = row[1];
							if (htblColNameValue.get(key) != null) {
								if (row[2].equals("java.awt.Polygon")) {

									if ((new DBpolygon((Polygon) tuple.get(in)))
											.compareTo(new DBpolygon((Polygon) (htblColNameValue.get(key)))) != 0) {
										found = false;
										break;
									}
								} else if (!(tuple.get(in).equals(htblColNameValue.get(key)))) {
									found = false;
									break;
								}
							}
							if (row[3].equals("true")) {
								// primaryKey=row[1];
								clusteringKeyIndex = in;
							}
						}
					}
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				if (found) {
					if (p.rows.size() == 1) {
						t.pageNames.remove(i);
						i--;
						pageExists = false;
						break;
						// t.nPages--;
						// t.pages.remove(i);
					} else {
						p.rows.remove(j);
						j--;
						p.min = p.rows.get(0).get(clusteringKeyIndex);
					}

				}
			}
			if (pageExists)
				serialize(p, t.pageNames.get(i));
		}
		serialize(t, strTableName);
	}

	@SuppressWarnings({ "unlikely-arg-type", "unchecked" })
	private void deleteRTree(Hashtable<String, Object> htblColNameValue, String strTableName, String indexedColName,
			int index) throws ClassNotFoundException, IOException {
		RTree rt = (RTree) deserialize(strTableName + indexedColName);
		Polygon pol = (Polygon) htblColNameValue.get(indexedColName);
		DBpolygon dbpol = new DBpolygon(pol);
		// Comparable key = dbpol.getArea();
		ArrayList<Ref> references = rt.search(dbpol);
		ArrayList<Ref> temp = new ArrayList<>();

		System.out.println(references.size());
		for (int i = 0; i < references.size(); i++) {
			Page pp = (Page) deserialize(references.get(i).getPageNo());
			if ((dbpol.equal((Polygon) pp.rows.get(references.get(i).getIndexInPage()).get(index)))) {
				temp.add(references.get(i));
			}
		}
		references = temp;
		System.out.println("size = " + temp.size());
		while (!(references.isEmpty())) {
			rt = (RTree) deserialize(strTableName + indexedColName);
			Ref ref = references.get(0);
			Page p = (Page) deserialize(ref.getPageNo());
			p.toString();
			System.out.println(ref.getIndexInPage());
			if (p.rows.size() > 0) {
				Vector<Object> tuple = p.rows.get(ref.getIndexInPage());
				String line;
				boolean found = true;
				int keyColIndex = -1;
				BufferedReader csvReader = new BufferedReader(new FileReader("metadata.csv"));
				while ((line = csvReader.readLine()) != null) {
					String[] row = line.split(",");
					if (row[0].equals(strTableName)) {
						keyColIndex++;
						String col = row[1];
						if (htblColNameValue.get(col) != null && !(col.equals(indexedColName))) {
							if (row[2].equals("java.awt.Polygon")) {
								if ((new DBpolygon((Polygon) tuple.get(keyColIndex)))
										.equal((Polygon) (htblColNameValue.get(col)))) {
									System.out.println("false");
									found = false;
									break;
								}
							} else if (!(tuple.get(keyColIndex).equals(htblColNameValue.get(col)))) {
								System.out.println(tuple.get(keyColIndex));
								System.out.println(htblColNameValue.get(col));
								System.out.println(col);
								System.out.println("false");
								found = false;
								break;
							}
						}
					}
				}

				if (found) {
					System.out.println(ref.getPageNo());
					System.out.println("found delete");
					// delete record from tree and page
					int indexOfrecord = ref.getIndexInPage();
					rt.delete(dbpol, ref);
					updateTrees(ref, strTableName, indexedColName);
					p.rows.remove(indexOfrecord);
					// p.toString();
					if (p.rows.isEmpty()) {
						Table t = (Table) deserialize(strTableName);
						t.pageNames.remove(ref.getPageNo());
						serialize(t, strTableName);
					}
					// update references in tree
					else {
						for (int i = indexOfrecord; i < p.rows.size(); i++) {
							DBpolygon keyValue = new DBpolygon((Polygon) p.rows.get(i).get(index));
							System.out.println(keyValue);
							ArrayList<Ref> otherKeys = rt.search(keyValue);
							Ref ref2 = new Ref(ref.getPageNo(), i);
							for (Ref r : otherKeys) {
								System.out.println("updating page" + i);
								if (r.getPageNo().equals(ref.getPageNo()) && r.getIndexInPage() == i + 1) {
									rt.delete(keyValue, r);
									rt.insert(keyValue, ref2);
									System.out.println("old index " + r.getIndexInPage());
									System.out.println("new index " + ref2.getIndexInPage());
								}
							}
							serialize(rt, strTableName + indexedColName);
						}
						references = rt.search(dbpol);
						for (int i = 0; i < references.size(); i++) {
							Page pp = (Page) deserialize(references.get(i).getPageNo());
							if ((dbpol.equal((Polygon) pp.rows.get(references.get(i).getIndexInPage()).get(index)))) {
								temp.add(references.get(i));
							}
						}
						references = temp;
						System.out.println("temp size" + temp.size());
					}
				}
			} else
				references.remove(0);
			serialize(p, ref.getPageNo());
		}
	}

	private void deleteBPTree(Hashtable<String, Object> htblColNameValue, String strTableName, String indexedColName,
			int index) throws ClassNotFoundException, IOException {
		BPTree bt = (BPTree) deserialize((strTableName + indexedColName));
		Comparable key = (Comparable) htblColNameValue.get(indexedColName);
		ArrayList<Ref> references = bt.search(key);

		while (!(references.isEmpty())) {
			Ref ref = references.get(0);
			Page p = (Page) deserialize(ref.getPageNo());
			p.toString();
			System.out.println("hi");
			System.out.println(ref.getPageNo());
			System.out.println(ref.getIndexInPage());
			System.out.println(p.rows.size());
			System.out.println(ref.getIndexInPage());
			if (p.rows.size() > 0) {
				Vector<Object> tuple = p.rows.get(ref.getIndexInPage());
				String line;
				boolean found = true;
				int keyColIndex = -1;
				BufferedReader csvReader = new BufferedReader(new FileReader("metadata.csv"));
				while ((line = csvReader.readLine()) != null) {
					String[] row = line.split(",");
					if (row[0].equals(strTableName)) {
						keyColIndex++;
						String col = row[1];
						if (htblColNameValue.get(col) != null) {
							if (row[2].equals("java.awt.Polygon")) {
								if ((new DBpolygon((Polygon) tuple.get(keyColIndex)))
										.compareTo(new DBpolygon((Polygon) (htblColNameValue.get(col)))) != 0) {
									found = false;
									break;
								}
							} else if (!(tuple.get(keyColIndex).equals(htblColNameValue.get(col)))) {
								System.out.println(tuple.get(keyColIndex));
								System.out.println(htblColNameValue.get(col));
								System.out.println(col);
								found = false;
								break;
							}
						}
					}
				}
				if (found) {
					System.out.println("found");
					// delete record from tree and page
					int indexOfrecord = ref.getIndexInPage();
					bt.delete(key, ref);
					references = bt.search(key);
					updateTrees(ref, strTableName, indexedColName);
					p.rows.remove(indexOfrecord);
					p.toString();
					if (p.rows.isEmpty()) {
						Table t = (Table) deserialize(strTableName);
						t.pageNames.remove(ref.getPageNo());
						serialize(t, strTableName);
					} else {
						// update references in tree
						for (int i = indexOfrecord; i < p.rows.size(); i++) {
							Comparable keyValue = (Comparable) p.rows.get(i).get(index);
							System.out.println(keyValue);
							ArrayList<Ref> otherKeys = bt.search(keyValue);
							Ref ref2 = new Ref(ref.getPageNo(), i);
							for (Ref r : otherKeys) {
								System.out.println("updating page" + i);
								if (r.getPageNo().equals(ref.getPageNo()) && r.getIndexInPage() == i + 1) {
									bt.delete(keyValue, r);
									bt.insert(keyValue, ref2);
									System.out.println("old index " + r.getIndexInPage());
									System.out.println("new index " + ref2.getIndexInPage());
								}
							}
						}
					}
				}
			} else
				references.remove(0);
			serialize(p, ref.getPageNo());
			serialize(bt, strTableName + indexedColName);
		}
	}

	private void updateTrees(Ref ref, String strTableName, String indexedColName)
			throws IOException, ClassNotFoundException {
		String line;
		BPTree bt;
		Page p;
		int index = -1;
		BufferedReader csvReader = new BufferedReader(new FileReader("metadata.csv"));
		while ((line = csvReader.readLine()) != null) {
			String[] row = line.split(",");
			if (row[0].equals(strTableName)) {
				index++;
				if (row[4].equals("true") && !(row[1].equals(indexedColName))) {
					bt = (BPTree) deserialize(strTableName + row[1]);
					p = (Page) deserialize(ref.getPageNo());
					Comparable key = (Comparable) p.rows.get(ref.getIndexInPage()).get(index);
					if (row[2].equals("java.awt.Polygon"))
						key = (new DBpolygon((Polygon) p.rows.get(ref.getIndexInPage()).get(index))).getArea();
					bt.delete(key, ref);
					// update references in tree
					for (int i = ref.getIndexInPage(); i < p.rows.size(); i++) {
						ArrayList<Ref> otherKeys = bt.search(key);
						Ref ref2 = new Ref(ref.getPageNo(), i);
						for (Ref r : otherKeys) {
							if (r.getPageNo().equals(ref.getPageNo()) && r.getIndexInPage() == i + 1) {
								bt.delete(key, r);
								bt.insert(key, ref2);
							}
						}
						serialize(bt, strTableName + row[1]);
					}
				}
			}
		}
	}

	public boolean checkTable(String strTableName) throws FileNotFoundException {
		Table t = null;
		String line;
		BufferedReader csvReader = new BufferedReader(new FileReader("metadata.csv"));
		try {
			while ((line = csvReader.readLine()) != null) {
				String[] row = line.split(",");
				if (row[0].equals(strTableName)) {
					return true;
				}
			}
		}

		catch (IOException e1) {
			e1.printStackTrace();
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	public void updateTable(String strTableName, String strClusteringKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, ClassNotFoundException, IOException {
		boolean tablefound = checkTable(strTableName);
		int index = -1;
		if (!tablefound)
			throw new DBAppException("table does not exist");
		System.err.println("In update");
		// Table t = (Table) deserialize(strTableName);
		String line;
		boolean indexed = false;
		ArrayList<String> indexedColName = new ArrayList<String>();
		String keyName = "";
		String type = "";
		// check if indexed column
		BufferedReader csvReader = new BufferedReader(new FileReader("metadata.csv"));
		while ((line = csvReader.readLine()) != null) {
			String[] row = line.split(",");
			if (row[0].equals(strTableName)) {
				index++;
				if (row[4].equals("true")) {
					if (row[3].equals("true")) {
						type = row[2];
						keyName = row[1];
						indexed = true;
					}
					if (htblColNameValue.containsKey(row[1]))
						indexedColName.add(row[1]);
				}

			}
		}
		if (indexed) {
			System.out.println("Indexed");
			updateTableIndexed(strTableName, strClusteringKey, htblColNameValue, indexedColName, index, keyName, type);
			return;
		} else {
			System.err.println("Not Indexed");
			updateTableNotIndexed(strTableName, strClusteringKey, htblColNameValue, indexedColName);
		}
	}

	public Comparable changeToComparable(String strTableName, String strClusteringKey, String type,
			ArrayList<String> indexedColName, int index, String keyString) {

		switch (type) {

		case "java.lang.Integer": {
			return (Comparable) Integer.parseInt(strClusteringKey);

		}
		case "java.lang.String": {
			return (Comparable) strClusteringKey;
		}
		case "java.lang.Double":
			return (Comparable) Double.parseDouble(strClusteringKey);

		case "java.lang.Boolean":
			return (Comparable) Boolean.parseBoolean(strClusteringKey);
		case "java.util.Date": {
			try {
				String tmp = strClusteringKey;
				Date key = (Date) new SimpleDateFormat("dd/MM/yyyy").parse(tmp);
				return (Comparable) key;

			} catch (Exception e) {

				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return (Comparable) Double.parseDouble(strClusteringKey);

		}
		case "java.awt.Polygon": {
			// String test = "(10,20),(30,30),(40,40),(50,60)";
			StringTokenizer st = new StringTokenizer(strClusteringKey, "),(", false);
			Polygon po = new Polygon();
			while (st.hasMoreTokens()) {
				po.addPoint(Integer.parseInt(st.nextToken()), Integer.parseInt(st.nextToken()));
			}
			DBpolygon db = new DBpolygon(po);

		}
		default:
			return null;
		}

	}

	private void updateTableIndexed(String strTableName, String strClusteringKey,
			Hashtable<String, Object> htblColNameValue, ArrayList<String> indexedColName, int index, String keyString,
			String type) throws DBAppException, ClassNotFoundException, IOException {
		BPTree bt;
		if (type.equals("java.util.Polygon")) {
			bt = (RTree) deserialize(strTableName + keyString);
		} else
			bt = (BPTree) deserialize(strTableName + keyString);
		Comparable key = changeToComparable(strTableName, strClusteringKey, type, indexedColName, index, keyString);
		ArrayList<Ref> references = bt.search(key);
		int clusteringKeyIndex = -1;
		String clusteringKeyType = "";
		ArrayList<String> ColName = new ArrayList<String>();

		while (!(references.size() == 0)) {

			Ref ref = references.get(0);
			Page p = (Page) deserialize(ref.getPageNo());
			Vector<Object> tuple = p.rows.get(ref.getIndexInPage());
			String line;
			boolean found = true;
			int keyColIndex = -1;
			BufferedReader csvReader = new BufferedReader(new FileReader("metadata.csv"));
			while ((line = csvReader.readLine()) != null) {
				String[] row = line.split(",");
				if (row[0].equals(strTableName)) {
					keyColIndex++;
					ColName.add(row[1]);
					if (htblColNameValue.get(ColName) != null) {
						if (row[2].equals("java.awt.Polygon")) {
							clusteringKeyIndex = keyColIndex;
							clusteringKeyType = row[2];
							if ((new DBpolygon((Polygon) tuple.get(keyColIndex)))
									.compareTo(new DBpolygon((Polygon) (htblColNameValue.get(ColName)))) != 0) {
								found = false;
								break;
							}
						} else if (!(tuple.get(keyColIndex).equals(htblColNameValue.get(ColName)))) {
							clusteringKeyIndex = keyColIndex;
							clusteringKeyType = row[2];
							System.out.println(tuple.get(keyColIndex));
							System.out.println(htblColNameValue.get(ColName));
							System.out.println(ColName);
							found = false;
							break;
						}
					}
				}
			}
			int j = ref.getIndexInPage();
			tuple.set(j, htblColNameValue.get(ColName.get(j)));
			DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
			LocalDateTime now = LocalDateTime.now();
			tuple.set(tuple.size() - 1, dtf.format(now));

//		if (found) {
//			references = bt.search(key);
//			int indexOfrecord = ref.getIndexInPage();
//			if (clusteringKeyType.equals("java.awt.Polygon")) {
//				RTree r = (RTree) bt;
//				references = r.search(key);
//				indexOfrecord = ref.getIndexInPage();
//				String test = "(10,20),(30,30),(40,40),(50,60)";
//				StringTokenizer st = new StringTokenizer(test, "),(", false);
//				Polygon po = new Polygon();
//				while (st.hasMoreTokens()) {
//					po.addPoint(Integer.parseInt(st.nextToken()), Integer.parseInt(st.nextToken()));
//				}
//				DBpolygon pol = new DBpolygon(po);
//				if (strClusteringKey.compareTo(p.min.toString()) >= 0) {
//					for (int k = 0; k < p.rows.size(); k++) {
//						// System.out.println(t.pages.get(i).rows.get(k).get(t.clusteringKeyIndex));
//						if (((DBpolygon) p.rows.get(k).get(clusteringKeyIndex)).compareTo(pol) == 0) {
//							// System.out.println("entered clustering key condition");
//							for (int j = 0; j < ColName.size(); j++) {
//								if (htblColNameValue.containsKey(ColName.get(j))) {
//									p.rows.get(k).set(j, htblColNameValue.get(ColName.get(j)));
//									DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
//									LocalDateTime now = LocalDateTime.now();
//									p.rows.get(k).set(p.rows.get(k).size() - 1, dtf.format(now));
//								}
//							}
//						}
//					}
//
//				}
//
//			} else {
//				if (strClusteringKey.compareTo(p.min.toString()) >= 0) {
//					for (int k = 0; k < p.rows.size(); k++) {
//						// System.out.println(t.pages.get(i).rows.get(k).get(t.clusteringKeyIndex));
//						if (p.rows.get(k).get(clusteringKeyIndex).toString().equals(strClusteringKey)) {
//							// System.out.println("entered clustering key condition");
//							for (int j = 0; j < ColName.size(); j++) {
//								if (htblColNameValue.containsKey(ColName.get(j))) {
//									p.rows.get(k).set(j, htblColNameValue.get(ColName.get(j)));
//									DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
//									LocalDateTime now = LocalDateTime.now();
//									p.rows.get(k).set(p.rows.get(k).size() - 1, dtf.format(now));
//								}
//							}
//						}
//					}
//
//				}
//			}

			updateTrees2(ref, strTableName, indexedColName, htblColNameValue);
			serialize(p, ref.getPageNo());
			references.remove(0);

		}
//		} else

		serialize(bt, strTableName + indexedColName);
	}

	@SuppressWarnings("unchecked")
	public void updateTableNotIndexed(String strTableName, String strClusteringKey,
			Hashtable<String, Object> htblColNameValue, ArrayList<String> indexedColNum)
			throws DBAppException, ClassNotFoundException, IOException {
		Table t = (Table) deserialize(strTableName);
		if (t == null)
			throw new DBAppException("table doesn't exist");
		ArrayList<String> ColName = new ArrayList<String>();
		ArrayList<Ref> refs = new ArrayList<Ref>();
		// Object primaryKey=null;
		int clusteringKeyIndex = -1;
		String clusteringKeyType = "";
		String pageName = "";
		int rowIndex = -1;
		int in = -1;
		String line;
		BufferedReader csvReader = new BufferedReader(new FileReader("metadata.csv"));
		try {
			while ((line = csvReader.readLine()) != null) {
				String[] row = line.split(",");
				if (row[0].equals(strTableName)) {

					in++;
					ColName.add(row[1]);
					// looking for the clustering key and its index in the vector of the record
					if (row[3].equals("true")) {
						// primaryKey=row[1];
						clusteringKeyIndex = in;
						clusteringKeyType = row[2];
					}
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		if (clusteringKeyType.equals("java.awt.Polygon")) {
			// String test = "(10,20),(30,30),(40,40),(50,60)";
			StringTokenizer st = new StringTokenizer(strClusteringKey, "),(", false);
			Polygon po = new Polygon();
			while (st.hasMoreTokens()) {
				po.addPoint(Integer.parseInt(st.nextToken()), Integer.parseInt(st.nextToken()));
			}
			DBpolygon pol = new DBpolygon(po);
			for (int i = t.pageNames.size() - 1; i > -1; i--) {
				Page p = (Page) deserialize(t.pageNames.get(i));

				if (strClusteringKey.compareTo(p.min.toString()) >= 0) {
					for (int k = 0; k < p.rows.size(); k++) {
						Polygon pp = (Polygon) p.rows.get(k).get(clusteringKeyIndex);
						DBpolygon poll = new DBpolygon(pp);
						// System.out.println(t.pages.get(i).rows.get(k).get(t.clusteringKeyIndex));
						if (poll.compareTo(pol) == 0) {
							System.out.println("entered clustering key condition");
							for (int j = 0; j < ColName.size(); j++) {
								if (htblColNameValue.containsKey(ColName.get(j))) {
									pageName = t.pageNames.get(i);
									rowIndex = k;
									Ref rr = new Ref(pageName, rowIndex);
									refs.add(rr);
									p.rows.get(k).set(j, htblColNameValue.get(ColName.get(j)));
									DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
									LocalDateTime now = LocalDateTime.now();
									p.rows.get(k).set(p.rows.get(k).size() - 1, dtf.format(now));
								}
							}
						}
					}

				}
				serialize(p, t.pageNames.get(i));
			}

			serialize(t, strTableName);
			return;
		} else {

			for (int i = t.pageNames.size() - 1; i > -1; i--) {
				Page p = (Page) deserialize(t.pageNames.get(i));
				if (strClusteringKey.compareTo(p.min.toString()) >= 0) {
					for (int k = 0; k < p.rows.size(); k++) {
						// System.out.println(t.pages.get(i).rows.get(k).get(t.clusteringKeyIndex));
						if (p.rows.get(k).get(clusteringKeyIndex).toString().equals(strClusteringKey)) {
							System.out.println("entered clustering key condition");
							for (int j = 0; j < ColName.size(); j++) {
								if (htblColNameValue.containsKey(ColName.get(j))) {
									pageName = t.pageNames.get(i);
									rowIndex = k;
									Ref rr = new Ref(pageName, rowIndex);
									refs.add(rr);
									p.rows.get(k).set(j, htblColNameValue.get(ColName.get(j)));
									DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
									LocalDateTime now = LocalDateTime.now();
									p.rows.get(k).set(p.rows.get(k).size() - 1, dtf.format(now));
								}
							}
						}
					}

				}
				serialize(p, t.pageNames.get(i));
			}
		}
		for (int l = 0; l < refs.size(); l++) {
			updateTrees2(refs.get(l), strTableName, indexedColNum, htblColNameValue);
		}
		serialize(t, strTableName);

	}

	private void updateTrees2(Ref ref, String strTableName, ArrayList<String> indexedColName,
			Hashtable<String, Object> htblColNameValue) throws IOException, ClassNotFoundException {
		String line;
		BPTree bt;
		Page p;
		int index = -1;
		String dataType = "";
		Comparable key;
		Comparable newKey;
		BufferedReader csvReader = new BufferedReader(new FileReader("metadata.csv"));
		for (int i = 0; i < indexedColName.size(); i++) {
			String ColName = indexedColName.get(i);
			while ((line = csvReader.readLine()) != null) {
				String[] row = line.split(",");
				if (row[0].equals(strTableName)) {
					dataType = row[2];
					index++;
					if (row[4].equals("true") && !(row[1].equals(ColName))) {
						if (dataType.equals("java.awt.Polygon")) {
							bt = (RTree) deserialize(strTableName + row[1]);
							p = (Page) deserialize(ref.getPageNo());
							Polygon poly = (Polygon) p.rows.get(ref.getIndexInPage()).get(index);
							DBpolygon DBpoly = new DBpolygon(poly);
							key = (Comparable) DBpoly;
							Polygon poly2 = (Polygon) htblColNameValue.get(ColName);
							DBpolygon DBpoly2 = new DBpolygon(poly2);
							newKey = (Comparable) DBpoly2;
						} else {
							bt = (BPTree) deserialize(strTableName + row[1]);
							p = (Page) deserialize(ref.getPageNo());
							key = (Comparable) p.rows.get(ref.getIndexInPage()).get(index);
							newKey = (Comparable) htblColNameValue.get(ColName);
						}
						bt.delete(key, ref);
						bt.insert(newKey, ref);

						serialize(bt, strTableName + row[1]);
					}
				}
			}
		}
	}

	public int binarySearch(int array[], int lowest, int highest, int x) {
		if (highest >= lowest) {
			int middle = lowest + (highest - lowest) / 2;
			if (array[middle] == x)
				return middle;
			if (array[middle] > x)
				return binarySearch(array, lowest, middle - 1, x);
			return binarySearch(array, middle + 1, highest, x);
		}
		return -1;
	}

	public void createBTreeIndex(String strTableName, String strColName) throws DBAppException, IOException {
		boolean tablefound = false, keyfound = false;
		Table table = null;
		int keyIndex = -1;
		String line;
		BufferedReader csvReader = new BufferedReader(new FileReader("metadata.csv"));
		try {
			while ((line = csvReader.readLine()) != null) {
				String[] row = line.split(",");
				if (row[0].equals(strTableName)) {
					tablefound = true;
					keyIndex++;
					if (row[1].contentEquals(strColName)) {
						keyfound = true;
						if (row[2].equals("java.awt.Polygon"))
							throw new DBAppException("can't create BPTree index on a polygon");
						break;
					}
				}
			}
		}

		catch (IOException e1) {
			e1.printStackTrace();
		}
		if (!tablefound)
			throw new DBAppException("table does not exist");
		try {
			table = (Table) deserialize(strTableName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (!keyfound)
			throw new DBAppException("there is no such Key in the table");

		FileReader reader = new FileReader("DBApp.properties");
		Properties pr = new Properties();
		pr.load(reader);
		nodeSize = Integer.parseInt(pr.getProperty("NodeSize"));
		BPTree b = new BPTree(nodeSize);
		Page p = null;
		for (int i = 0; i < table.pageNames.size(); i++) {
			try {
				p = (Page) deserialize(table.pageNames.get(i));
			} catch (Exception e) {
				e.printStackTrace();
			}
			for (int j = 0; j < p.rows.size(); j++) {
				Ref r = new Ref(table.pageNames.get(i), j);
				b.insert((Comparable) p.rows.get(j).get(keyIndex), r);
			}

		}
		table.BPTNames.add(strColName);
		serialize(b, strTableName + strColName);
		String old = "";
		String row;
		csvReader = new BufferedReader(new FileReader("metadata.csv"));
		try {
			while ((row = csvReader.readLine()) != null) {
				String[] rowSplit = row.split(",");
				if (rowSplit[0].equals(strTableName) && rowSplit[1].contentEquals(strColName)) {
					old += rowSplit[0] + "," + rowSplit[1] + "," + rowSplit[2] + "," + rowSplit[3] + "," + true + "\n";
				} else {
					old += row + "\n";
				}
			}
			csvReader.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try (PrintWriter writer = new PrintWriter("metadata.csv")) {
			StringBuilder sb = new StringBuilder();
			sb.append(old);

			writer.write(sb.toString());
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		}

		try {
			serialize(table, strTableName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void createRTreeIndex(String strTableName, String strColName)
			throws DBAppException, ClassNotFoundException, IOException {
		String dataType = "";
		FileReader reader = new FileReader("DBApp.properties");
		Properties pr = new Properties();
		pr.load(reader);
		nodeSize = Integer.parseInt(pr.getProperty("NodeSize"));
		int keyIndex = 0;
		String line;
		BufferedReader csvReader = new BufferedReader(new FileReader("metadata.csv"));
		line = csvReader.readLine();
		while ((line = csvReader.readLine()) != null) {
			String[] row = line.split(",");
			if (row[0].equals(strTableName)) {
				if (row[1].equals(strColName)) {
					dataType = row[2];
					break;
				} else
					keyIndex++;
			} else
				keyIndex = 0;
		}
		if (dataType.equals("java.awt.Polygon")) {
			String old = "";
			String row;
			csvReader = new BufferedReader(new FileReader("metadata.csv"));
			try {
				while ((row = csvReader.readLine()) != null) {
					String[] rowSplit = row.split(",");
					if (rowSplit[0].equals(strTableName) && rowSplit[1].contentEquals(strColName)) {
						old += rowSplit[0] + "," + rowSplit[1] + "," + rowSplit[2] + "," + rowSplit[3] + "," + true
								+ "\n";
					} else {
						old += row + "\n";
					}
				}
				csvReader.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			try (PrintWriter writer = new PrintWriter("metadata.csv")) {
				StringBuilder sb = new StringBuilder();
				sb.append(old);

				writer.write(sb.toString());
			} catch (FileNotFoundException e) {
				System.out.println(e.getMessage());
			}
			RTree b = new RTree(nodeSize);
			Table t = (Table) deserialize(strTableName);
			for (int j = 0; j < t.pageNames.size(); j++) {
				Page p = (Page) deserialize(t.pageNames.get(j));

				for (int i = 0; i < p.rows.size(); i++) {
					Ref recordReference = new Ref(t.pageNames.get(j), i);
					DBpolygon poly = new DBpolygon((Polygon) (p.rows.get(i).get(keyIndex)));
					b.insert(poly, recordReference);
				}
				serialize(p, t.pageNames.get(j));
			}

			serialize(b, strTableName + strColName);
			t.RTNames.add(strColName);
			serialize(t, strTableName);
			b.toString();
		} else {
			DBAppException d = new DBAppException("This Type is incompatiable with rtree");
		}
	}

}
