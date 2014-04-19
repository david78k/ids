% approach1: convert into a vector of size 300
% and merge all the files into a single csv file
val = 'train' ;
%val = 'test' ;
dirPath = [val '/Class'] ;
basedir = ['csvfiles/'] ;
outDirPath = [basedir, dirPath] ;
nClasses = 6 ;

fileName = [val '.csv'] ;
fullFileName = [basedir, fileName] 

cmd = ["rm -rf " basedir]
system(cmd) ;
cmd = ["mkdir -p " basedir]
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
        imgReshaped = [img(:,:,1) ; img(:,:,2) ; img(:,:,3)] ;
        %splitImgName = regexp(imgName, '\.', 'split') ; % for matlab
        %splitImgName = strsplit(imgName, '\.') ; % for octave
        %imgNameWithoutExtension = splitImgName{1} ;
        %fileName = [imgNameWithoutExtension '.csv'] ;
        %dmlwrite(fullFileName, idivide(imgReshaped, 16, "floor"), '-append') ; % for matlab
        csvwrite(fullFileName, idivide(imgReshaped, 16, "floor"), '-append') ; % for octave
        %csvwrite(fullFileName, idivide(imgReshaped, 16, "floor")) ;
        %csvwrite(fullFileName, imgReshaped) ;
    end
end

