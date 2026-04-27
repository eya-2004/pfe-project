package com.example.demo;

public class Stats {
	
		private int totalDocs;
		private int totalErrors;
		private int totalSucces;
		private float score;
		public Stats(int totalDocs,int totalErrors,int totalSucces,float score) {
			this.totalDocs=totalDocs;
			this.totalErrors=totalErrors;
			this.totalSucces=totalSucces;
			this.score=score;
		}
		public int getTotaldocs() {
			return totalDocs;
		}
		
		public void setTotaldocs(int totalDocs) {
			this.totalDocs=totalDocs;
		}
		public int getTotalerrors() {
			return totalErrors;
		}
		public void setTotalerrors(int totalErrors) {
			this.totalErrors=totalErrors;
		}
		public int getTotalsucces() {
			return totalSucces;
		}
		public void setTotalsucces(int totalSucces) {
			this.totalSucces=totalSucces;
		}
		public float getScore() {
			return score;
		}
		public void setScore(float score) {
			this.score=score;
		}
	
	}


