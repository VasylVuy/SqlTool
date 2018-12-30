package com.sql.tool;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class SqlGen {

	public static void main(String[] args) throws IOException {		
		ComparisonData data = ComparisonData.parse(args);
		
		PrintStream stream = System.out;
		
		File file = new File("c:\\temp\\" + data.getFileName());
		file.createNewFile();
		stream = new PrintStream(new BufferedOutputStream(new FileOutputStream(file, false)));

		stream.println("-- only in left --");
		stream.println(data.generateOnlyInLeft());
		
		stream.println();
		stream.println();
		stream.println("-- only in right --");
		stream.println(data.generateOnlyInRight());
		
		stream.println();
		stream.println();
		stream.println("-- partial discrepancy --");
		stream.println(data.generatePartialDiscrepancy());
		
		stream.close();
	}
	
	public static class ComparisonData{
		public static String LeftAlias = "tl";
		public static String RightAlias = "tr";
		
		private List<String> leftPrimaryKey;
		private List<String> rightPrimaryKey;
		private List<String> leftColumns;
		private List<String> rightColumns;
		private String leftTableName;
		private String rightTableName;
		
		public ComparisonData(String leftTableName, String rightTableName, 
				List<String> leftPrimaryKey, List<String> rightPrimaryKey, 
				List<String> leftColumns, List<String> rightColumns) {
			this.leftTableName = leftTableName;
			this.rightTableName = rightTableName;
			this.leftPrimaryKey = leftPrimaryKey;
			this.rightPrimaryKey = rightPrimaryKey;
			this.leftColumns = leftColumns;
			this.rightColumns = rightColumns;
		}
		
		public List<String> getLeftPrimaryKey(){
			return leftPrimaryKey;
		}

		public List<String> getRightPrimaryKey() {
			return rightPrimaryKey;
		}

		public List<String> getLeftColumns() {
			return leftColumns;
		}

		public List<String> getRightColumns() {
			return rightColumns;
		}

		public String getLeftTableName() {
			return leftTableName;
		}

		public String getRightTableName() {
			return rightTableName;
		}
		
		private String getLeftSelect() {
			StringBuilder selectLeftColumns = new StringBuilder();
			
			for(int i = 0; i < getLeftColumns().size(); i++) {
				if(selectLeftColumns.length() > 0) {
					selectLeftColumns.append(", ");
				}
				
				selectLeftColumns.append(LeftAlias).append(".").append(getLeftColumns().get(i));
			}
			
			return selectLeftColumns.toString();
		}
		
		private String getRightSelect() {
			StringBuilder selectRightColumns = new StringBuilder();
			
			for(int i = 0; i < getRightColumns().size(); i++) {
				if(selectRightColumns.length() > 0) {
					selectRightColumns.append(", ");
				}
				
				selectRightColumns.append(LeftAlias).append(".").append(getRightColumns().get(i));
			}
			
			return selectRightColumns.toString();
		}
		
		private String getPrimaryKeyJoin(String lAlias, String rAlias) {
			StringBuilder primaryKeyJoin = new StringBuilder();
			
			for(int i = 0; i < getLeftPrimaryKey().size(); i++) {
				if(primaryKeyJoin.length() > 0) {
					primaryKeyJoin.append(" and ");
				}
				
				primaryKeyJoin.append(lAlias).append(".").append(getLeftPrimaryKey().get(i)).append(" = ");
				primaryKeyJoin.append(rAlias).append(".").append(getRightPrimaryKey().get(i));
			}
			
			return primaryKeyJoin.toString();
		}
		
		private String getAllColumnsJoin() {
			StringBuilder allColumnsJoin = new StringBuilder();
			
			for(int i = 0; i < getRightColumns().size(); i++) {
				if(allColumnsJoin.length() > 0) {
					allColumnsJoin.append(" and ");
				}
				
				allColumnsJoin.append("(" + LeftAlias + "." + getLeftColumns().get(i) + " = " + RightAlias + "." + getRightColumns().get(i) + " or ");
				allColumnsJoin.append("(" + LeftAlias + "." + getLeftColumns().get(i) + " is NULL and " + RightAlias + "." + getRightColumns().get(i) + " is NULL )) \r\n");
			}
			
			return allColumnsJoin.toString();
		}
		
		private String getWhereLeftPrimaryIsNull() {
			StringBuilder primaryKeyIsNull = new StringBuilder();
			
			for(int i = 0; i < getLeftPrimaryKey().size(); i++) {
				if(primaryKeyIsNull.length() > 0) {
					primaryKeyIsNull.append(" and ");
				}
				
				primaryKeyIsNull.append(LeftAlias).append(".").append(getLeftPrimaryKey().get(i)).append(" is null");
			}
			
			return primaryKeyIsNull.toString();
		}
		
		private String getWhereRightPrimaryIsNull() {
			StringBuilder primaryKeyIsNull = new StringBuilder();
			
			for(int i = 0; i < getRightPrimaryKey().size(); i++) {
				if(primaryKeyIsNull.length() > 0) {
					primaryKeyIsNull.append(" and ");
				}
				
				primaryKeyIsNull.append(RightAlias).append(".").append(getRightPrimaryKey().get(i)).append(" is null");
			}
			
			return primaryKeyIsNull.toString();
		}
		
		public String generateOnlyInLeft() {
			return "Select " + getLeftSelect() + "\r\n" +
					"From " + getLeftTableName() + " " + LeftAlias + " left outer join " + getRightTableName() + " " + RightAlias + "\r\n" +
					"on " + getPrimaryKeyJoin(LeftAlias, RightAlias) + "\r\n" +
					"where " + getWhereRightPrimaryIsNull();
 		}
		
		public String generateOnlyInRight() {
			return "Select " + getRightSelect() + "\r\n" +
					"From " + getLeftTableName() + " " + LeftAlias + " right outer join " + getRightTableName() + " " + RightAlias + "\r\n" +
					"on " + getPrimaryKeyJoin(LeftAlias, RightAlias) + "\r\n" +
					"where " + getWhereLeftPrimaryIsNull();
 		}
		
		
		public String generatePartialDiscrepancy() {
			String leftSideJoin = "leftSideJoin";
			String rightSideJoin = "rightSideJoin";
			
			return "Select * from \r\n" 
					+ "(Select " + getLeftSelect() + "\r\n" +
						"From " + getLeftTableName() + " " + LeftAlias + " left outer join " + getRightTableName() + " " + RightAlias + "\r\n" +
						"on " + getAllColumnsJoin() +
						"where " + getWhereRightPrimaryIsNull() + ") " + leftSideJoin + " inner join \r\n" + 
					   "(Select " + getRightSelect() + "\r\n" +
						"From " + getLeftTableName() + " " + LeftAlias + " right outer join " + getRightTableName() + " " + RightAlias + "\r\n" +
						"on " + getAllColumnsJoin() +
						"where " + getWhereLeftPrimaryIsNull() + ") " + rightSideJoin + "\r\n" +
						"on " + getPrimaryKeyJoin(leftSideJoin, rightSideJoin);
 		}
		
		public String getFileName() {
			SimpleDateFormat simpleDateFormat =
		            new SimpleDateFormat("MMddhhmmss");
			String dateAsString = simpleDateFormat.format(new Date());
		
			return getLeftTableName() + "_" + getRightTableName() + "_" + dateAsString + ".txt";
		}
		
		public static ComparisonData parse(String[] args) {
			
			if(args.length < 6) {
				throw new IllegalArgumentException("There should be 6 arguments");
			}
			
			String leftTableName = args[0];
			String rightTableName = args[1];			
			
			List<String> leftPrimaryKey = Arrays.asList(args[2].split(","));
			List<String> rightPrimaryKey = Arrays.asList(args[3].split(","));
			List<String> leftColumns = Arrays.asList(args[4].split(","));
			List<String> rightColumns = Arrays.asList(args[5].split(","));
			
			return new ComparisonData(leftTableName, rightTableName, leftPrimaryKey, rightPrimaryKey, leftColumns, rightColumns);
		}
	}

}

