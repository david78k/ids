% approach1: convert into a vector of size 300
% and merge all the files into a single csv file
val = 'train' ;
%val = 'test' ;
dirPath = [val '/Class'] ;
outDirPath = ['csvfiles/' dirPath] ;
nClasses = 6 ;
for i = 1 : nClasses
    dirPath1 = [dirPath,' ', num2str(i), '/'] 
    %outDirPath1 = [outDirPath,' ', num2str(i), '/'] 
    outDirPath1 = [outDirPath, num2str(i), '/'] 
    %mkdir(outDirPath1) 
    cmd = ["rm -rf " outDirPath1]
    system(cmd) ;
    cmd = ["mkdir -p " outDirPath1]
    system(cmd) ;
    F = dir(dirPath1) ;
    for j =  1 : length(F)-2
        imgName = F(j+2).name ;
        fullImgName = strcat(dirPath1, imgName) ;
        img = imread(fullImgName) ;
        [r, c, d] = size(img) ;
        imgReshaped = [img(:,:,1) ; img(:,:,2) ; img(:,:,3)] ;
        %splitImgName = regexp(imgName, '\.', 'split') ;
        splitImgName = strsplit(imgName, '\.') ;
        imgNameWithoutExtension = splitImgName{1} ;
        fileName = [imgNameWithoutExtension '.csv'] ;
        fullFileName = [outDirPath1, fileName] ;
        csvwrite(fullFileName, idivide(imgReshaped, 16, "floor")) ;
        %csvwrite(fullFileName, floor(imgReshaped/16.0)) ;
        %csvwrite(fullFileName, imgReshaped) ;
    end
end

