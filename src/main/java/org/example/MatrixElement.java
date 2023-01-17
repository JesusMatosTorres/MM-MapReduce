package org.example;

import org.apache.hadoop.io.Text;

public class MatrixElement {
    private int row;
    private int col;
    private int value;
    private int numCols;
    private int[][] matrix;

    public MatrixElement(int row, int col, int value, int numCols) {
        this.row = row;
        this.col = col;
        this.value = value;
        this.numCols = numCols;
        this.matrix = new int[row][col];
    }

    public int getRow() {
        return row;
    }

    public int getCol() {
        return col;
    }

    public int getValue(int row, int col) {
        return matrix[row][col];
    }

    public int getNumCols() {
        return numCols;
    }

    public static MatrixElement fromText(Text value) {
        String[] parts = value.toString().split(",");
        int row = Integer.parseInt(parts[0]);
        int col = Integer.parseInt(parts[1]);
        int val = Integer.parseInt(parts[2]);
        int numCols = Integer.parseInt(parts[3]);
        return new MatrixElement(row, col, val, numCols);
    }
}
