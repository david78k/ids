% approach1: convert into a vector of size 300
% and merge all the files into a single csv file
%val = 'train' ;
val = 'test' ;
dirPath = [val '/Class'] ;
basedir = ['csvfiles/'] ;
outDirPath = [basedir, dirPath] ;
nClasses = 6 ;

fileName = [val '-cluster.csv'] ;
fullFileName = [basedir, fileName] 

%cmd = ["rm -rf " basedir]
%system(cmd) ;
cmd = ["mkdir -p " basedir]
system(cmd) ;
cmd = ["rm -rf " fullFileName]
system(cmd) ;

for i = 1 : nClasses
    dirPath1 = [dirPath,' ', num2str(i), '/'] 
    F = dir(dirPath1) ;
    for j =  1 : length(F)-2
        imgName = F(j+2).name ;
        fullImgName = strcat(dirPath1, imgName) ;
        img = imread(fullImgName) ;
        [r, c, d] = size(img) ;
        imgReshaped = [img(:,:,1)  img(:,:,2)  img(:,:,3)] ;
	R = idivide(img(:,:,1), 16, "floor");
	G = idivide(img(:,:,2), 16, "floor");
	B = idivide(img(:,:,3), 16, "floor");
	for k = 1: r*c 
		row = [R(k) G(k) B(k)];
        	csvwrite(fullFileName, row, '-append') ; % for octave
	end	
    end
end

