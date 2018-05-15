include "alldifferent.mzn";
include "element.mzn";

int: n;
set of int: points = 1..n;
array [1..n] of var 1..n: x; % queen is column i is in row q[i]
array [1..n] of var 1..n: y; % queen is column i is in row q[i]
array [1..n] of var 1..n: z; % queen is column i is in row q[i]
array [1..n] of var 1..n: temp; % queen is column i is in row q[i]
array [1..n, 1..n] of var int: scores; 
var int: total_score; 
array[1..n, 1..n] of int: match12;
array[1..n, 1..n] of int: match23;
array[1..n, 1..n] of int: match31;

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
    scores[2,i] = match23[i, y[i]] /\
    scores[3,i] = match31[i, z[i]]    
);

constraint total_score = sum(i in points, j in points)(scores[i, j]);

% search
% solve :: int_search(x, first_fail, indomain_min, complete)
%       satisfy;

solve minimize total_score;
output [ show(x[i]) ++ ", " | i in points] ++ ["\n"] ++ 
       [ show(y[i]) ++ ", " | i in points] ++ ["\n"] ++ 
       [ show(z[i]) ++ ", " | i in points] ++ ["\n"] ++
       [ show(scores[i, j]) ++ ", " | i,j in points] ++ ["\n"] ++
       [ show(total_score)];