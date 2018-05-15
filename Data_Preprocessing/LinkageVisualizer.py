from pymongo import MongoClient
import jellyfish
import json
import numpy as np
from LinkageSimilarity import LinkageSimilarity
from LinkageFactory import LinkageFactory

class LinkageVisualizer:
    def __init__(self, linkageFactory):
        self.linkageFactory = linkageFactory
        self.scores = []
        self.total_score = -1
        self.linkages = []
        self.num_db = -1
        self.num_link = -1

    def getLinkageFactory(self):
        return self.linkageFactory

    def parseStringToIntArray(self, line):
        start = line.find("[")
        end = line.find("]")
        line = line[start+1:end]
        raw_scores = line.split(", ")
        result = []
        for raw_score in raw_scores:
            result.append(int(raw_score))
        
        return result
    
    def readOutput(self, dir):
        self.num_db = len(self.getLinkageFactory().getLinkages())
        self.num_link = len(self.getLinkageFactory().getLinkages()[0].getSourceDocs())

        file = open(dir, "r")          
        line = ""        

        #First_line - scores
        line = file.readline()
        scores = self.parseStringToIntArray(line)
        self.scores = np.reshape(scores, (self.num_db, self.num_link))
                
        #Second_line - total score
        line = file.readline()
        self.total_score = int(line.split(" = ")[1].split(";")[0])

        linkages = []
        
        while(True):
            line = file.readline()
            if "x" not in line: 
                break
            linkage = self.parseStringToIntArray(line)
            linkages.append(linkage)            
        
        self.linkages = linkages
        file.close()  
    
    def visualize(self, dir):
        
        file = open(dir, "w")          
        for i in range(0, self.num_link):
            line = ""
            current_index = i + 1 #in minizinc count from 1
            source = self.getLinkageFactory().getLinkages()[0].sourceDocs_vals[current_index - 1]
            line += "{0}|".format(source)            
            for j in range(1, self.num_db):
                current_index = self.linkages[j-1][current_index - 1]
                source = self.getLinkageFactory().getLinkages()[j].sourceDocs_vals[current_index - 1]
                line += "{0}|".format(source)            
            file.write("{0}\n".format(line))

if __name__ == '__main__':

    cols = ["Google_Hotels", "Yelp_Hotels", "TripAdvisor_Hotels"]
    linkageFactory = LinkageFactory()
    linkageFactory.loadFromGeneralOutputs(cols, "dataset/")
    linkageFactory.toMinizincOutput("../Minizinc_Model/linkage.dzn")

    linkageVisualizer = LinkageVisualizer(linkageFactory)
    linkageVisualizer.readOutput("../Minizinc_Model/output")
    linkageVisualizer.visualize("../Minizinc_Model/visualize")
    


    
