from pymongo import MongoClient
import jellyfish
import json
import numpy as np
import shutil
import os
from LinkageSimilarity import LinkageSimilarity

class LinkageFactory:
    def __init__(self):
        self.linkages = []

    def getLinkages(self):
        return self.linkages

    def generateDataSet(self, cols, rootDir, sampleSize):

        linkages = []

        #Cycle cross check
        linkage = LinkageSimilarity()
        linkage.setMongoConfig(config)
        linkage.setRootDir(rootDir)
        linkage.setSourceDocs(cols[0])
        linkage.sampleItemsFromSource(sampleSize)
        linkage.setDestDocs(cols[1])
        linkage.calculateScore()
        linkage.filterBySimilarity(0.2)
        linkage.toGeneralOutput()
        linkages.append(linkage)

        for i in range(1,len(cols) - 1):
            linkage = LinkageSimilarity()
            linkage.setMongoConfig(config)
            linkage.setRootDir(rootDir)
            linkage.setSourceDocs(linkages[i-1].getDestDocs())
            linkage.setSourceName(linkages[i-1].getDestName())
            linkage.setDestDocs(cols[i+1])
            linkage.calculateScore()    
            linkage.filterBySimilarity(0.2)
            linkage.toGeneralOutput()
            linkages.append(linkage)

        linkage = LinkageSimilarity()
        linkage.setRootDir(rootDir)
        linkage.setSourceDocs(linkages[(len(cols)-2)].getDestDocs())
        linkage.setSourceName(linkages[(len(cols)-2)].getDestName())
        linkage.setDestDocs(linkages[0].getSourceDocs())
        linkage.setDestName(linkages[0].getSourceName())
        linkage.calculateScore()    
        linkage.filterBySimilarity(0.2)        
        linkage.toGeneralOutput()
        linkages.append(linkage)

        #Finalize - regenerate the score
        linkage = LinkageSimilarity()
        linkage.setRootDir(rootDir)
        linkage.setSourceDocs(linkages[(len(cols)-1)].getDestDocs())
        linkage.setSourceName(linkages[(len(cols)-1)].getDestName())
        linkage.setDestDocs(linkages[1].getSourceDocs())
        linkage.setDestName(linkages[1].getSourceName())
        linkage.calculateScore()
        linkage.toGeneralOutput()
        linkages[0] = linkage
        
        for i in range(1,len(cols) - 1):
            linkage = LinkageSimilarity()
            linkage.setRootDir(rootDir)
            linkage.setSourceDocs(linkages[i-1].getDestDocs())
            linkage.setSourceName(linkages[i-1].getDestName())
            linkage.setDestDocs(linkages[i+1].getSourceDocs())
            linkage.setDestName(linkages[i+1].getSourceName())
            linkage.calculateScore()
            linkage.toGeneralOutput()
            linkages[i] = linkage
        
        self.linkages = linkages

        rootDir = "../dataset/"
        for i in range(0, len(cols) - 1):
            rootDir += "{0}_".format(len(linkages[i].getSourceDocs()))
        rootDir += "{0}/".format(len(linkages[len(cols)-1].getSourceDocs()))
        
        for i in range(0, len(cols)) :
            os.makedirs(os.path.dirname(rootDir + linkages[i].generalOutputDir), exist_ok=True)
            shutil.move(linkages[i].rootDir + linkages[i].generalOutputDir, rootDir + linkages[i].generalOutputDir)
        
        print("Data files are stored in folder {0}".format(rootDir))

    def loadFromGeneralOutputs(self, cols, rootDir = ""):
        
        linkages = []

        linkage = LinkageSimilarity()
        linkage.setRootDir(rootDir)
        linkage.setSourceName(cols[0])
        linkage.setDestName(cols[1])
        linkage.loadGeneralOutput()
        linkages.append(linkage)

        for i in range(1,len(cols) - 1):
            linkage = LinkageSimilarity()
            linkage.setRootDir(rootDir)
            linkage.setSourceName(linkages[i-1].getDestName())
            linkage.setDestName(cols[i+1])
            linkage.loadGeneralOutput()
            linkages.append(linkage)

        linkage = LinkageSimilarity()
        linkage.setRootDir(rootDir)
        linkage.setSourceName(linkages[(len(cols)-2)].getDestName())
        linkage.setDestName(cols[0])
        linkage.loadGeneralOutput()
        linkages.append(linkage)

        self.linkages = linkages

    def toMinizincOutput(self, rootDir = "", dir = ""):
        linkages = self.linkages
            
        if(dir == ""):
            dir = "linkage_"
            for i in range(0, len(cols) - 1):
                dir += "{0}_".format(len(linkages[i].getSourceDocs()))
            dir += "{0}.dzn".format(len(linkages[len(cols)-1].getSourceDocs()))
            
        file = open(rootDir + dir, "w")          
        
        #instances
        file.write("n = " + str(len(linkages[0].getSourceDocs())) + ";\n")
        for i in range(1,len(linkages)):
            file.write("n{0} = {1};\n".format(str(i+1), str(len(linkages[i].getSourceDocs()))))
        
        #scores        
        for i in range(0, len(linkages)):

            scores = linkages[i].getScores()
            lenSource = len(linkages[i].getSourceDocs())
            lenDest = len(linkages[i].getDestDocs())

            if i != (len(linkages) - 1):
                file.write("match{0}{1} = [|\n".format(i+1, i+2))
                #First lenSource - 1 lines
                for j in range(0, lenSource - 1):
                    for k in range(0, lenDest - 1):
                        file.write("{:.0f}, ".format(scores[j, k] * 100))
                        # if k == 3:
                        #     break
                    file.write("{:.0f}|\n".format(scores[j, lenDest - 1] * 100))            
                    # if j == 3:
                    #     break
                #Last lines
                for k in range(0, lenDest - 1):
                    file.write("{:.0f}, ".format(scores[lenSource - 1, k] * 100))
                    # if k == 3:
                    #     break
                file.write("{:.0f}|];\n".format(scores[lenSource - 1, lenDest - 1] * 100))            

            else: #If the match is from last db to first db, generate [lenSource * lenSource] score matrix

                tempScores = np.zeros((lenSource, lenSource))
                for i in range(0, lenSource):
                    for j in range(0, lenDest):
                        tempScores[i, j] = scores[i, j]
                    for j in range(lenDest, lenSource):                        
                        tempScores[i, j] = 1.00
                
                scores = tempScores
                lenDest = lenSource

                file.write("match{0}{1} = [|\n".format(len(linkages), 1))
                #First lenSource - 1 lines
                for j in range(0, lenSource - 1):
                    for k in range(0, lenDest - 1):
                        file.write("{:.0f}, ".format(scores[j, k] * 100))
                        # if k == 3:
                        #     break
                    file.write("{:.0f}|\n".format(scores[j, lenDest - 1] * 100))            
                    # if j == 3:
                    #     break
                #Last lines
                for k in range(0, lenDest - 1):
                    file.write("{:.0f}, ".format(scores[lenSource - 1, k] * 100))
                    # if k == 3:
                    #     break
                file.write("{:.0f}|];\n".format(scores[lenSource - 1, lenDest - 1] * 100))            
                
        file.close() 
        print("Minizinc dzn file is stored in dir: {0}".format(rootDir + dir))


if __name__ == '__main__':

    cols = ["Google_Hotels", "Yelp_Hotels", "TripAdvisor_Hotels"]
    config = {}
    config["HOST"] = "localhost"
    config["GATE"] = 27017
    config["DB_NAME"] = "google_review"
    linkageFactory = LinkageFactory()
    linkageFactory.generateDataSet(cols, "../dataset/", 50)
    # linkageFactory.loadFromGeneralOutputs(cols, "dataset/")
    linkageFactory.toMinizincOutput(rootDir = "../Minizinc_Model/")
    
    
        
    