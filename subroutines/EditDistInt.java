package ca.uvic.csc.research;

import java.lang.String;
import java.lang.Integer;
import java.util.Arrays;

/* This is a class implementing Ukkonen's improved edit distance algorithm. 
	For more information, please read, "Algorithms for Approximate String Matching", Esko Ukkonen, Dept. of Computer Science, University of Helsinki, Finland. Published in 'Information and Control', Vol. 64, Nos. 1-3, Jan-Mar 1985. */

class EditDistInt { 

	public static int distanceTest(int[] one, int[] two, int threshold){
		return distanceTest(one, two, threshold, 2); //default substitution cost is 2
	}

	public static int distanceTest(int[] one, int[] two, int threshold, int substitutionCost){

		if(one.length > two.length){ //shortest string first, please
			int[] swap = one;
			one = two;
			two = swap;
		}

		int length_difference = two.length - one.length; //diff. in lengths of two strings
		if(length_difference > threshold){ //if the difference in lengths is too long, it's not in the threshold - trivial case
			return -1;
		}
		int free_swaps = (threshold - length_difference) / 2; //what this is about: if there's a certain difference in string lengths, then (difference) moves must be assigned to additions on the short string or deletions in the long string to equalize the lengths. For the rest, any addition must be matched up with a deletion later to keep the lengths equalized. Alternately, as a substitution is costed as equal to one addition + one deletion, this is the number of substitutions that can be done.
	
		int i;
	
		int one_direction_move_threshold_bottom = free_swaps+1; //+1 because the boundary is <, not <=
		int one_direction_move_threshold_top = (free_swaps+length_difference)*-1;
	
		int boundary_top = 0;
		int boundary_bottom = (one.length+1 > threshold) ? threshold+1 : one.length+1; //as the cost of a step is 1, there is a boundary in the starting column where the cost cannot exceed the threshold. If this threshold is not reached in the column, the boundary is temporarily tied to the bottom of the column. +1 as the boundary check is <, not <=.
		int next_boundary_top, next_boundary_bottom; 
	
		int[] column = new int[one.length+1];
		int[] nextColumn = new int[one.length+1];
		Arrays.fill(nextColumn, Integer.MAX_VALUE);
		for(i=0; i<one.length+1; i++){
			column[i] = i; //fill initial column with cost in steps: 0, 1, 2, 3, etc
		}
		int progress = 0;
	
		// The goal is to reach the END POINT of the matrix, that is where progress == two.length and we're at the bottom of the column. For the least cost, obviously. If there's no route less than [threshold] cost, we can stop now.
	
		int upper_turn_threshold = Math.max(boundary_top, one_direction_move_threshold_top+progress);
		int lower_turn_threshold = Math.min(boundary_bottom, one_direction_move_threshold_bottom+progress);
		if(upper_turn_threshold > one.length+1){ return -1; }
		
		while(progress < two.length && upper_turn_threshold <= lower_turn_threshold){
		//System.out.println(progress+":"+Arrays.toString(column)+":"+upper_turn_threshold+":"+(lower_turn_threshold-1));
			next_boundary_top = -1;
			next_boundary_bottom = -1;
		
			for(i=upper_turn_threshold; i<lower_turn_threshold; i++){
				//to the right
				if(i == upper_turn_threshold || column[i]+1 < nextColumn[i]){
					
					nextColumn[i] = column[i]+1; //addition
					//System.out.println(column[i]+" at place "+i+" becomes "+nextColumn[i]+" (R)");
					if(nextColumn[i] <= threshold){
						if(next_boundary_top == -1){ next_boundary_top = i; }
						if(next_boundary_bottom <= i){ next_boundary_bottom = i+1; /*System.out.println("(updating lower boundary to "+(i+1)+")");*/}
					}	 
				}
				//down and right
				if(i+1 < one.length+1){
					if(one[i] == two[progress]){
						nextColumn[i+1] = column[i]; // match, free pass
						//System.out.println(column[i]+" at place "+i+" becomes "+nextColumn[i+1]+" (DR - match)");
					} else {
						nextColumn[i+1] = column[i]+substitutionCost; //no match, substitution
						//System.out.println(column[i]+" at place "+i+" becomes "+nextColumn[i+1]+" (DR - no match)");
					}
					if(nextColumn[i+1] <= threshold){
						if(next_boundary_top == -1){ next_boundary_top = i; }
						if(next_boundary_bottom <= i+1){ next_boundary_bottom = i+2; /*i+1+1*/ /*System.out.println("(updating lower boundary to "+(i+2)+")");*/}
					}	
				}
				//below
				if(i+1 < lower_turn_threshold && column[i+1] > column[i]+1){
					column[i+1] = column[i]+1; //deletion
					//System.out.println(column[i]+" below place "+i+" becomes "+column[i+1]+" (D)");
				}
			}
			//recalculate upper and lower thresholds
			progress++;
			boundary_top = next_boundary_top;
			boundary_bottom = next_boundary_bottom;
			upper_turn_threshold = Math.max(boundary_top, one_direction_move_threshold_top+progress);
			lower_turn_threshold = Math.min(boundary_bottom, one_direction_move_threshold_bottom+progress);
			if(upper_turn_threshold > one.length+1){ return -1; //this is awkward. If the upper turn threshold is below the bottom of the array, we inevitably must report failure. 
			}
			if(lower_turn_threshold > one.length+2){ lower_turn_threshold = one.length+2; } //i.e., one after the end of the array so we go all the way to the bottom of the column
			column = nextColumn;
			nextColumn = new int[one.length+1];
			Arrays.fill(nextColumn, Integer.MAX_VALUE);
		}
		//while ends - if we're not in the last column, return failure.
		//System.out.println(progress+":"+Arrays.toString(column));
		
		if(progress < two.length){ return -1; }
		if(upper_turn_threshold > one.length+1){ return -1; }
	
		for(i=upper_turn_threshold; i<one.length+1; i++){
			if(i+1 < one.length+1 && column[i+1] > column[i]+1){
					column[i+1] = column[i]+1; //deletion to finish off
			}
		}
		if(column[one.length] <= threshold){ return column[one.length]; } //if the last item is within the threshold, return how long the distance is.
		return -1; //otherwise, fail
	}
	
	public static void main(String[] args){ //test suite for the string version
		/*System.out.println(distanceTest("kitten","sitting",7)); //works with 5
		System.out.println(distanceTest("kitten","sitting",3)); //fails
		System.out.println(distanceTest("carebear","areabear",3)); //works with 2
		System.out.println(distanceTest("carebear","areabear",1)); //fails
		System.out.println(distanceTest("antidisestablishmentarianism","antihistamines",20)); //works with 18
		System.out.println(distanceTest("antidisestablishmentarianism","anathemises",20)); //fails - min is 22
		System.out.println(distanceTest("antidisestablishmentarianism","blandishments",20)); //works with 19
		*/
	}
	
}