% approach2: generate a histogram along the three color dimensions
% each with 16 levels. the size of the resulting vector is 48.
%val = 'train' ;
val = 'test' ;
dirPath = [val '/Class'] ;
basedir = ['csvfiles/'] ;
outDirPath = [basedir, dirPath] ;
nClasses = 6 ;

fileName = [val '.csv'] ;
fullFileName = [basedir, fileName] 

%cmd = ["rm -rf " basedir]
%system(cmd) ;
cmd = ["mkdir -p " basedir]
system(cmd) ;
cmd = ["rm -rf " fullFileName]
system(cmd) ;

for i = 1 : nClasses
    dirPath1 = [dirPath,' ', num2str(i), '/'] 
    %outDirPath1 = [outDirPath,' ', num2str(i), '/'] 
    %outDirPath1 = [outDirPath, num2str(i), '/'] 
    %mkdir(outDirPath1) % for matlab
    %cmd = ["rm -rf " outDirPath1]
    %system(cmd) ;
    %cmd = ["mkdir -p " outDirPath1]
    %system(cmd) ;
    F = dir(dirPath1) ;
    for j =  1 : length(F)-2
        imgName = F(j+2).name ;
        fullImgName = strcat(dirPath1, imgName) ;
        img = imread(fullImgName) ;
        [r, c, d] = size(img) ;
        %imgReshaped = [img(:,:,1) ; img(:,:,2) ; img(:,:,3)] ;
        imgReshaped = [img(:,:,1)  img(:,:,2)  img(:,:,3)] ;
	R = idivide(img(:,:,1), 16, "floor");
	G = idivide(img(:,:,2), 16, "floor");
	B = idivide(img(:,:,3), 16, "floor");
	%row = [R G B];
	row = zeros(1,48);
	%row = [i]
	for k = 1: r*c 
		% for each level count the number of pixels
		row(1, R(k) + 1) ++;
		row(16 + G(k) + 1) ++;
		row(32 + B(k) + 1) ++;
	end	
        csvwrite(fullFileName, row, '-append') ; % for octave
        %csvwrite(fullFileName, idivide(row, 16, "floor"), '-append') ; % for octave

        %splitImgName = regexp(imgName, '\.', 'split') ; % for matlab
        %splitImgName = strsplit(imgName, '\.') ; % for octave
        %imgNameWithoutExtension = splitImgName{1} ;
        %fileName = [imgNameWithoutExtension '.csv'] ;
        %csvwrite(fullFileName, idivide(imgReshaped, 16, "floor"), '-append') ; % for octave
        %csvwrite(fullFileName, idivide(imgReshaped, 16, "floor")) ;
    end
end

