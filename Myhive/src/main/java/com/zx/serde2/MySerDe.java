package com.zx.serde2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import jline.internal.Log;

public class MySerDe extends AbstractSerDe {
	// params
	private List<String> columnNames = null;
	private List<TypeInfo> columnTypes = null;
	private ObjectInspector objectInspector = null;
	private String lineSep = null;
	private String kvSep = null;

	@Override
	public void initialize(Configuration conf, Properties tbl) throws SerDeException {
		// Read sep
		lineSep = "\n";
		kvSep = ",";
		tbl.getProperty(Constants.SERIALIZATION_NULL_FORMAT, "");

		// Read Column Names
		String columnNameProp = tbl.getProperty(Constants.LIST_COLUMNS);
		if (columnNameProp != null && columnNameProp.length() > 0) {
			columnNames = Arrays.asList(columnNameProp.split(","));
		} else {
			columnNames = new ArrayList<String>();
		}

		// Read Column Types
		String columnTypeProp = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
		// default all string
		if (columnTypeProp == null) {
			String[] types = new String[columnNames.size()];
			Arrays.fill(types, 0, types.length, Constants.STRING_TYPE_NAME);
			columnTypeProp = StringUtils.join(types, ":");
		}
		columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProp);

		// Check column and types equals
		if (columnTypes.size() != columnNames.size()) {
			throw new SerDeException("len(columnNames) != len(columntTypes)");
		}

		// Create ObjectInspectors from the type information for each column
		List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>();
		ObjectInspector oi;
		for (int c = 0; c < columnNames.size(); c++) {
			oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(c));
			columnOIs.add(oi);
		}
		objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);

	}

	@Override
	public Object deserialize(Writable wr) throws SerDeException {
		// Split to kv pair
		if (wr == null)
			return null;
		Map<String, String> kvMap = new HashMap<String, String>();
		Text text = (Text) wr;
		Log.info("fristTEXT = " + text.toString());
		if (text.toString().startsWith("<DOC>") && text.toString().endsWith("</DOC>")) {

			text.set(text.toString().substring(5, text.toString().lastIndexOf("/") - 1));
		}

		for (String kv : text.toString().split(lineSep)) {
			String[] pair = kv.split(kvSep);
			Log.info("text = " + kv);
			if (pair.length == 2) {
				String[] pair2 = pair[0].split("=");
				String[] pair3 = pair[1].split("=");
				Log.info("pair2 = " + pair2[0] + " + " + pair2[1]);
				kvMap.put(pair2[0], pair2[1]);
				kvMap.put(pair3[0], pair3[1]);
			}
		}

		// Set according to col_names and col_types
		ArrayList<Object> row = new ArrayList<Object>();
		String colName = null;
		TypeInfo type_info = null;
		Object obj = null;
		for (int i = 0; i < columnNames.size(); i++) {
			colName = columnNames.get(i);
			type_info = columnTypes.get(i);
			obj = null;
			if (type_info.getCategory() == ObjectInspector.Category.PRIMITIVE) {
				PrimitiveTypeInfo p_type_info = (PrimitiveTypeInfo) type_info;
				switch (p_type_info.getPrimitiveCategory()) {
				case STRING:
					obj = StringUtils.defaultString(kvMap.get(colName), "");
					break;
				case LONG:
				case INT:
					try {
						obj = Integer.parseInt(kvMap.get(colName));
					} catch (Exception e) {
					}
				}
			}
			row.add(obj);
		}
		return row;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return objectInspector;
	}

	@Override
	public SerDeStats getSerDeStats() {
		return null;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return Text.class;
	}

	@Override
	public Writable serialize(Object arg0, ObjectInspector arg1) throws SerDeException {
		return null;
	}
}