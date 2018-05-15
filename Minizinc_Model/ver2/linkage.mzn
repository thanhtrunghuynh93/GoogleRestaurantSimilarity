include "alldifferent.mzn";
include "element.mzn";

int: n;
int: n2;
int: n3;
set of int: points = 1..n;
set of int: points2 = 1..n2;
set of int: points3 = 1..n3;
array [1..n] of var 1..n2: x;  
array [1..n2] of var 1..n3: y;  
array [1..n3] of var 1..n3: z;

array [1..n] of var 1..n3: temp;  

array [1..n, 1..n] of var int: scores; 
var int: total_score; 
array[1..n, 1..n2] of int: match12;
array[1..n2, 1..n3] of int: match23;
array[1..n3, 1..n3] of int: match31;

constraint alldifferent(x);
constraint alldifferent(y);
constraint alldifferent(z);

%Cycle constraint
constraint forall (i in points) (
    element(x[i], y, temp[i]) /\ element(temp[i], z, i)
);

%Minimize the point
constraint forall (i in points) (
    scores[1,i] = match12[i, x[i]] /\
    scores[2,i] = match23[x[i], y[x[i]]] /\
    scores[3,i] = match31[y[x[i]], z[y[x[i]]]]   
);

constraint total_score = sum(i in points, j in points)(scores[i, j]);

%search
solve :: int_search(x, first_fail, indomain_min, complete)
     minimize total_score;
%      satisfy;

output [ show(x[i]) ++ ", " | i in points] ++ ["\n"] ++ 
       [ show(y[i]) ++ ", " | i in points2] ++ ["\n"] ++ 
       [ show(z[i]) ++ ", " | i in points3] ++ ["\n"] ++
       [ show(scores[i, j]) ++ ", " | i,j in points] ++ ["\n"] ++
       [ show(total_score)];
