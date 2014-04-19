val = 'train' ;
dirPath = [val '/Class'] ;
outDirPath = ['csvfiles/' dirPath] ;
nClasses = 6 ;
for i = 1 : nClasses
    dirPath1 = [dirPath,' ', num2str(i), '/'] ;
    outDirPath1 = [outDirPath,' ', num2str(i), '/'] ;
    mkdir(outDirPath1) ;
    F = dir(dirPath1) ;
    for j =  1 : length(F)-2
        imgName = F(j+2).name ;
        fullImgName = strcat(dirPath1, imgName) ;
        img = imread(fullImgName) ;
        [r, c, d] = size(img) ;
        imgReshaped = [img(:,:,1) ; img(:,:,2) ; img(:,:,3)] ;
        splitImgName = regexp(imgName, '\.', 'split') ;
        imgNameWithoutExtension = splitImgName{1} ;
        fileName = [imgNameWithoutExtension '.csv'] ;
        fullFileName = [outDirPath1, fileName] ;
        csvwrite(fullFileName, imgReshaped) ;
    end
end

