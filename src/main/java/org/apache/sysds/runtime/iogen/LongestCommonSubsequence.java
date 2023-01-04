/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.runtime.iogen;

import org.apache.spark.sql.sources.In;
import org.apache.sysds.lops.Lop;

import java.util.BitSet;

import java.util.ArrayList;

public class LongestCommonSubsequence {
	public ArrayList<String> getAllLCS(String str1, String str2) {
		int m = str1.length();
		int n = str2.length();
		BitSet[] lcsMatrix = new BitSet[m];

		ArrayList<String> allLCS = new ArrayList<>();
		for(int i = 0; i < m; i++)
			lcsMatrix[i] = new BitSet(n);

		for(int i = 0; i < m; i++) {
			for(int j = 0; j < n; j++) {
				if((str1.charAt(i) + "").equals(Lop.OPERAND_DELIMITOR) ||
					(str2.charAt(j) + "").equals(Lop.OPERAND_DELIMITOR))
					continue;
				if(str1.charAt(i) == str2.charAt(j)) {
					lcsMatrix[i].set(j, true);
				}
			}
		}
		for(int i = 0; i < m; i++) {
			if((str1.charAt(i) + "").equals(Lop.OPERAND_DELIMITOR) || lcsMatrix[i].cardinality() == 0)
				continue;
			else {
				for(int j = lcsMatrix[i].nextSetBit(0); j != -1; j = lcsMatrix[i].nextSetBit(j + 1)) {
					StringBuilder sb = new StringBuilder();
					sb.append(str1.charAt(i));
					int l = j + 1;
					for(int k = i + 1; k < m && l < n; k++) {
						if(lcsMatrix[k].cardinality() == 0 || lcsMatrix[k].nextSetBit(l) == -1) {
							if(!sb.toString().endsWith(Lop.OPERAND_DELIMITOR))
								sb.append(Lop.OPERAND_DELIMITOR);
							continue;
						}
						int ul = lcsMatrix[k].nextSetBit(l);
						if(ul != -1) {
							int lul = ul -1;
							int lk = k-1;
							if(ul - l > 1 || (lul > 0 && lk>0 && !lcsMatrix[lk].get(lul))) {
								if(!sb.toString().endsWith(Lop.OPERAND_DELIMITOR))
									sb.append(Lop.OPERAND_DELIMITOR);
							}
							sb.append(str2.charAt(ul));
							l = ul + 1;
						}
					}
					if(sb.length() > 0 && !sb.toString().equals(Lop.OPERAND_DELIMITOR))
						allLCS.add(sb.toString());
				}
			}
		}
		return allLCS;
	}

	private int getCardinalityOfRow(boolean[][] lcsMatrix, int rowIndex){
		int c = 0;
		for(Boolean b: lcsMatrix[rowIndex])
			if(b)
				c++;
		return c;
	}
	private int getCardinalityOfCol(boolean[][] lcsMatrix, int colIndex){
		int c = 0;
		for(boolean[] matrix : lcsMatrix)
			if(matrix[colIndex])
				c++;
		return c;
	}

	private ArrayList<Integer> getAllValuesOfRow(boolean[][] lcsMatrix, int rowIndex){
		ArrayList<Integer> result = new ArrayList<>();
		int index = 0;
		for(Boolean b: lcsMatrix[rowIndex]) {
			if(b)
				result.add(index);
			index++;
		}
		return result;
	}

	private ArrayList<Integer> getAllValuesOfCol(boolean[][] lcsMatrix, int colIndex){
		ArrayList<Integer> result = new ArrayList<>();
		int index = 0;
		for(boolean[] matrix : lcsMatrix) {
			if(matrix[colIndex])
				result.add(index);
			index++;
		}
		return result;
	}

	private int getNextSetCol(boolean[][] lcsMatrix, int rowIndex, int colIndex){
		int result = -1;
		for(int i=colIndex; i<lcsMatrix[0].length && result==-1; i++)
			result = lcsMatrix[rowIndex][i] ? i : -1;
		return result;
	}

	private int getNextSetRow(boolean[][] lcsMatrix, int rowIndex, int colIndex){
		int result = -1;
		for(int i=rowIndex; i<lcsMatrix.length && result==-1; i++)
			result = lcsMatrix[i][colIndex] ? i : -1;
		return result;
	}
	public ArrayList<String> getLCS(String str1, String str2) {
		int m = str1.length();
		int n = str2.length();
		boolean[][] lcsMatrix = new boolean[m][n];

		ArrayList<String> allLCS = new ArrayList<>();
		for(int i = 0; i < m; i++)
			for(int j=0; j<n; j++)
				lcsMatrix[i][j] = false;

		for(int i = 0; i < m; i++) {
			for(int j = 0; j < n; j++) {
				if((str1.charAt(i) + "").equals(Lop.OPERAND_DELIMITOR) ||
					(str2.charAt(j) + "").equals(Lop.OPERAND_DELIMITOR)) {
					continue;
				}
				if(str1.charAt(i) == str2.charAt(j)) {
					lcsMatrix[i][j] = true;
				}
			}
		}
		// layout 1: row-col
		for(int i = 0; i < m; i++) {
			if((str1.charAt(i) + "").equals(Lop.OPERAND_DELIMITOR) ||  getCardinalityOfRow(lcsMatrix, i) == 0)
				continue;
			else {

				for(Integer j: getAllValuesOfRow(lcsMatrix, i)) {
					int li = i -1;
					int lj = j-1;
					if(li > 0 && lj>0 && lcsMatrix[li][lj])
						continue;
					StringBuilder sb = new StringBuilder();
					sb.append(str1.charAt(i));
					int l = j + 1;
					for(int k = i + 1; k < m && l < n; k++) {
						int ul = getNextSetCol(lcsMatrix, k, l);
						if(getCardinalityOfRow(lcsMatrix, k) == 0 || ul == -1) {
							if(!sb.toString().endsWith(Lop.OPERAND_DELIMITOR))
								sb.append(Lop.OPERAND_DELIMITOR);
							continue;
						}
						int lul = ul -1;
						int lk = k-1;
						if(ul - l > 0 || (lul > 0 && lk>0 && !lcsMatrix[lk][lul])) {
								if(!sb.toString().endsWith(Lop.OPERAND_DELIMITOR))
									sb.append(Lop.OPERAND_DELIMITOR);
						}
						sb.append(str2.charAt(ul));
						l = ul + 1;
					}
					if(sb.length() > 0 && !sb.toString().equals(Lop.OPERAND_DELIMITOR)) {
						allLCS.add(sb.toString());
					}
				}
			}
		}
		/////////////////////////////
		// layout 2: col-row
		for(int j = 0; j < n; j++) {
			if((str2.charAt(j) + "").equals(Lop.OPERAND_DELIMITOR) || getCardinalityOfCol(lcsMatrix, j) == 0)
				continue;
			else {
				for(Integer i: getAllValuesOfCol(lcsMatrix, j)) {
					int li = i -1;
					int lj = j-1;
					if(li > 0 && lj>0 && lcsMatrix[li][lj])
						continue;
					StringBuilder sb = new StringBuilder();
					sb.append(str2.charAt(j));
					int l = i + 1;
					for(int k = j + 1; k < n && l < m; k++) {
						int ul = getNextSetRow(lcsMatrix, l, k);
						if(getCardinalityOfCol(lcsMatrix, k) == 0 || ul == -1) {
							if(!sb.toString().endsWith(Lop.OPERAND_DELIMITOR))
								sb.append(Lop.OPERAND_DELIMITOR);
							continue;
						}
						int lul = ul -1;
						int lk = k-1;
						if(ul - l > 0 || (lul > 0 && lk > 0 && !lcsMatrix[lul][lk])) {
							if(!sb.toString().endsWith(Lop.OPERAND_DELIMITOR))
								sb.append(Lop.OPERAND_DELIMITOR);
						}
						sb.append(str1.charAt(ul));
						l = ul + 1;
					}
					if(sb.length() > 0 && !sb.toString().equals(Lop.OPERAND_DELIMITOR)) {
						allLCS.add(sb.toString());
					}
				}
			}
		}
		return allLCS;
	}

	public ArrayList<String> getLCS3(String str1, String str2, int j) {
		int m = str1.length();
		int n = str2.length();
		boolean[][] lcsMatrix = new boolean[m][n];

		ArrayList<String> allLCS = new ArrayList<>();
		for(int i = 0; i < m; i++)
			for(int jj=0; jj<n; jj++)
				lcsMatrix[i][jj] = false;

		for(int i = 0; i < m; i++) {
			for(int jj = 0; jj < n; jj++) {
				if((str1.charAt(i) + "").equals(Lop.OPERAND_DELIMITOR) ||
					(str2.charAt(jj) + "").equals(Lop.OPERAND_DELIMITOR)) {
					continue;
				}
				if(str1.charAt(i) == str2.charAt(jj)) {
					lcsMatrix[i][jj] = true;
				}
			}
		}
		// layout 1: row-col
		for(int i = 0; i < m; i++) {
			if((str1.charAt(i) + "").equals(Lop.OPERAND_DELIMITOR) ||  getCardinalityOfRow(lcsMatrix, i) == 0)
				continue;
			else {

				//for(Integer j: getAllValuesOfRow(lcsMatrix, i)) {
					int li = i -1;
					int lj = j-1;
					if(li > 0 && lj>0 && lcsMatrix[li][lj])
						continue;
					StringBuilder sb = new StringBuilder();
					sb.append(str1.charAt(i));
					int l = j + 1;
					int lastJ = -1;
					for(int k = i + 1; k < m && l < n; k++) {
						int ul = getNextSetCol(lcsMatrix, k, l);
						if(getCardinalityOfRow(lcsMatrix, k) == 0 || ul == -1) {
							if(!sb.toString().endsWith(Lop.OPERAND_DELIMITOR))
								sb.append(Lop.OPERAND_DELIMITOR);
							continue;
						}
						int lul = ul -1;
						int lk = k-1;
						if(ul - l > 0 || (lul > 0 && lk>0 && !lcsMatrix[lk][lul])) {
							if(!sb.toString().endsWith(Lop.OPERAND_DELIMITOR))
								sb.append(Lop.OPERAND_DELIMITOR);
						}
						sb.append(str2.charAt(ul));
						l = ul + 1;
						lastJ = ul;
					}
					if(sb.length() > 0 && !sb.toString().equals(Lop.OPERAND_DELIMITOR)) {
						allLCS.add(sb.toString());
						if(sb.toString().contains("PID")){
							System.out.println(lastJ+" >> "+sb.toString());
						}
					}
				//}
			}
		}
		/////////////////////////////
		// layout 2: col-row
		//for(int j = 0; j < n; j++) {
			if((str2.charAt(j) + "").equals(Lop.OPERAND_DELIMITOR) || getCardinalityOfCol(lcsMatrix, j) == 0)
				;
			else {
				for(Integer i: getAllValuesOfCol(lcsMatrix, j)) {
					int li = i -1;
					int lj = j-1;
					if(li > 0 && lj>0 && lcsMatrix[li][lj])
						continue;
					StringBuilder sb = new StringBuilder();
					sb.append(str2.charAt(j));
					int l = i + 1;
					int lastJ = -1;
					for(int k = j + 1; k < n && l < m; k++) {
						int ul = getNextSetRow(lcsMatrix, l, k);
						if(getCardinalityOfCol(lcsMatrix, k) == 0 || ul == -1) {
							if(!sb.toString().endsWith(Lop.OPERAND_DELIMITOR))
								sb.append(Lop.OPERAND_DELIMITOR);
							continue;
						}
						int lul = ul -1;
						int lk = k-1;
						if(ul - l > 0 || (lul > 0 && lk > 0 && !lcsMatrix[lul][lk])) {
							if(!sb.toString().endsWith(Lop.OPERAND_DELIMITOR))
								sb.append(Lop.OPERAND_DELIMITOR);
						}
						sb.append(str1.charAt(ul));
						l = ul + 1;
						lastJ = ul;
					}
					if(sb.length() > 0 && !sb.toString().equals(Lop.OPERAND_DELIMITOR)) {
						allLCS.add(sb.toString());
						if(sb.toString().contains("PID")){
							System.out.println(lastJ+" >+> "+sb.toString());
						}
					}
				}
			//}
		}
		return allLCS;
	}

	public ArrayList<String> getLCS2(String str1, String str2) {
		int m = str1.length();
		int n = str2.length();
		BitSet[] lcsMatrix = new BitSet[m];

		ArrayList<String> allLCS = new ArrayList<>();
		for(int i = 0; i < m; i++) {
			lcsMatrix[i] = new BitSet(n);
		}

		for(int i = 0; i < m; i++) {
			for(int j = 0; j < n; j++) {
				if((str1.charAt(i) + "").equals(Lop.OPERAND_DELIMITOR) ||
					(str2.charAt(j) + "").equals(Lop.OPERAND_DELIMITOR))
					continue;
				if(str1.charAt(i) == str2.charAt(j)) {
					lcsMatrix[i].set(j, true);
				}
			}
		}
		for(int i = 0; i < m; i++) {
			if((str1.charAt(i) + "").equals(Lop.OPERAND_DELIMITOR) || lcsMatrix[i].cardinality() == 0)
				continue;
			else {
				for(int j = lcsMatrix[i].nextSetBit(0); j != -1; j = lcsMatrix[i].nextSetBit(j + 1)) {
					int li = i -1;
					int lj = j-1;
					if(li > 0 && lj>0 && lcsMatrix[li].get(lj))
						continue;
					StringBuilder sb = new StringBuilder();
					sb.append(str1.charAt(i));
					int l = j + 1;
					for(int k = i + 1; k < m && l < n; k++) {
						if(lcsMatrix[k].cardinality() == 0 || lcsMatrix[k].nextSetBit(l) == -1) {
							if(!sb.toString().endsWith(Lop.OPERAND_DELIMITOR))
								sb.append(Lop.OPERAND_DELIMITOR);
							continue;
						}
						if(sb.toString().contains("PID")){
							int ggg = 1000;
						}
						int ul = lcsMatrix[k].nextSetBit(l);
						if(ul != -1) {
							int lul = ul -1;
							int lk = k-1;
							if(ul - l > 1 || (lul > 0 && lk>0 && !lcsMatrix[lk].get(lul))) {
								if(!sb.toString().endsWith(Lop.OPERAND_DELIMITOR))
									sb.append(Lop.OPERAND_DELIMITOR);
							}
							sb.append(str2.charAt(ul));
							l = ul + 1;
						}
					}
					if(sb.length() > 0 && !sb.toString().equals(Lop.OPERAND_DELIMITOR))
						allLCS.add(sb.toString());
				}
			}
		}
		return allLCS;
	}

}

//class LCS_ALGO {
//  static void lcs(String S1, String S2, int m, int n) {
//    int[][] LCS_table = new int[m + 1][n + 1];
//
//    // Building the mtrix in bottom-up way
//    for (int i = 0; i <= m; i++) {
//      for (int j = 0; j <= n; j++) {
//        if (i == 0 || j == 0)
//          LCS_table[i][j] = 0;
//        else if (S1.charAt(i - 1) == S2.charAt(j - 1))
//          LCS_table[i][j] = LCS_table[i - 1][j - 1] + 1;
//        else
//          LCS_table[i][j] = Math.max(LCS_table[i - 1][j], LCS_table[i][j - 1]);
//      }
//    }
//
//    int index = LCS_table[m][n];
//    int temp = index;
//
//    char[] lcs = new char[index + 1];
//    lcs[index] = '\0';
//
//    int i = m, j = n;
//    while (i > 0 && j > 0) {
//      if (S1.charAt(i - 1) == S2.charAt(j - 1)) {
//        lcs[index - 1] = S1.charAt(i - 1);
//
//        i--;
//        j--;
//        index--;
//      }
//
//      else if (LCS_table[i - 1][j] > LCS_table[i][j - 1])
//        i--;
//      else
//        j--;
//    }
//
//    // Printing the sub sequences
//    System.out.print("S1 : " + S1 + "\nS2 : " + S2 + "\nLCS: ");
//    for (int k = 0; k <= temp; k++)
//      System.out.print(lcs[k]);
//    System.out.println("");
//  }
//
//  public static void main(String[] args) {
//    String S1 = "ACADB";
//    String S2 = "CBDA";
//    int m = S1.length();
//    int n = S2.length();
//    lcs(S1, S2, m, n);
//  }
//}
