from pymongo import MongoClient
import jellyfish
import json
import numpy as np
import random

class LinkageSimilarity:
    def __init__(self):
        self.rootDir = ""
        self.sourceName = ""
        self.destName = ""
        self.dict = {}
        self.sourceDocs = {}
        self.destDocs = {}
        self.sourceDocs_keys = [] 
        self.sourceDocs_vals = []        
        self.destDocs_keys = [] 
        self.destDocs_vals = []        
        self.scores = []
        self.generalOutputDir = ""

    def setMongoConfig(self, config):
        self.client = MongoClient(config["HOST"], config["GATE"])
        self.db = self.client[config["DB_NAME"]]            

    def setRootDir(self, rootDir):
        self.rootDir = rootDir

    #Read data from db to dictionary
    def readColFromDB(self, col):
        collection = self.db[col]        
        docsCur = collection.find()        
        docs = {}

        i = 0
        for doc in docsCur:
            if(doc["Name"] != None):
                docs[str(doc["_id"])] = doc["Name"]
                
        return docs

    def setSourceName(self, name):
        self.sourceName = name
    def getSourceName(self):
        return self.sourceName
    def setDestName(self, name):
        self.destName = name
    def getDestName(self):
        return self.destName

    def setSourceDocs(self, source):
        if type(source) is str:
            self.sourceName = source
            self.sourceDocs = self.readColFromDB(self.sourceName)
        else:
            self.sourceDocs = source
            
        self.sourceDocs_keys = [] 
        self.sourceDocs_vals = []        
        for key1, str1 in self.sourceDocs.items():
            self.sourceDocs_keys.append(key1)
            self.sourceDocs_vals.append(str1)
    
    def sampleItemsFromSource(self, n):
        samples_keys = random.sample(self.sourceDocs_keys, n)
        samples_vals = []
        samples_docs = {}

        for key in samples_keys:
            val = self.sourceDocs[key]
            samples_docs[key] = val 
            samples_vals.append(val)            
        
        self.sourceDocs = samples_docs
        self.sourceDocs_keys = samples_keys
        self.sourceDocs_vals = samples_vals
    
    def setDestDocs(self, dest):        
        if type(dest) is str:
            self.destName = dest
            self.destDocs = self.readColFromDB(self.destName)    
        else:
            self.destDocs = dest            

        self.destDocs_keys = []        
        self.destDocs_vals = []                       
        for key2, str2 in self.destDocs.items():
            self.destDocs_keys.append(key2)
            self.destDocs_vals.append(str2)

    def getSourceDocs(self):        
        return self.sourceDocs
    
    def getDestDocs(self):        
        return self.destDocs

    def getScores(self):
        return self.scores
    
    def calculateScore(self):
        print("-----------------------------------------------------------------")
        print("Generating similarity dictionary between " + self.sourceName + " and " + self.destName)        

        self.scores = np.zeros((len(self.sourceDocs), len(self.destDocs)))        

        #Calculate and store the scores
        i = 0
        
        for i in range(0, len(self.sourceDocs)):         
            str1 = self.sourceDocs_vals[i]
            for j in range(0, len(self.destDocs)):         
                str2 = self.destDocs_vals[j]
                self.scores[i , j] = self.levenshtein_score(str1, str2)                
            if (i % 100 == 0):
                print("Calculated for " + str(i) + " instances")        
        print("Calculated for " + str(i) + " instances")        

    def checkScoreSanity(self):
        scores = np.zeros((len(self.sourceDocs), len(self.destDocs)))     
        print("-----------------------------------------------------------------")
        print("Checking sanity")        
        for i in range(0, len(self.sourceDocs)):         
            str1 = self.sourceDocs_vals[i]
            for j in range(0, len(self.destDocs)):         
                str2 = self.destDocs_vals[j]
                scores[i , j] = self.levenshtein_score(str1, str2)                
                if(scores[i , j] - self.levenshtein_score(str1, str2) > 0.01):
                    print("{0} {1} unsanity".format(i, j))

    #Filter score, keep only source that 
    def filterBySimilarity(self, threshold):
        print("-----------------------------------------------------------------")
        print("Filtering similarity dictionary with threshold " + str(threshold))        

        filteredSource = {}
        filteredDest = {}
        
        for i in range(0, len(self.sourceDocs)):
            if(np.min(self.scores[i,:]) <= threshold):
                filteredSource[self.sourceDocs_keys[i]] = self.sourceDocs_vals[i]                
                # count = 0
                for j in range(0, len(self.destDocs)):
                    if(self.scores[i,j] <= threshold):
                        # print("{0} - {1} = {2} | {3}".format(self.sourceDocs_vals[i], self.destDocs_vals[j], self.scores[i, j], self.levenshtein_score(self.sourceDocs_vals[i], self.destDocs_vals[j])))
                        filteredDest[self.destDocs_keys[j]] = self.destDocs_vals[j]               
                #         count = count + 1
                # if(count > 1):
                #     print(count)

        print("There are " + str(len(filteredSource)) + " instances remaining in source")       
        print("and " + str(len(filteredDest)) + " instances remaining in destination")         
        
        
        self.setSourceDocs(filteredSource)
        self.setDestDocs(filteredDest)
        self.calculateScore()    

    def levenshtein_score(self, str1, str2):
        s1 = str1.lower()
        s2 = str2.lower()
        sim = jellyfish.levenshtein_distance(str1.lower(), str2.lower())
        
        #priotize the case that s1 contains s2
        if((s1 in s2) or (s2 in s1)):
            sim = sim / 3

        return sim / max(len(str1), len(str2))

    def toGeneralOutput(self, dir = ""):
        
        if dir == "":           
            dir = self.sourceName + "_" + self.destName
        
        self.generalOutputDir = dir

        file = open(self.rootDir + dir, "w")          
        
        file.write(str(len(self.sourceDocs)) + " " + str(len(self.destDocs)) + "\n")
        
        #Collection 1
        file.write("--- Collection 1 ---\n")
        for i in range(0, len(self.sourceDocs)):
            file.write(self.sourceDocs_keys[i] + " " + self.sourceDocs_vals[i] + "\n")

        #Collection 2
        file.write("--- Collection 2 ---\n")
        for i in range(0, len(self.destDocs)):
            file.write(self.destDocs_keys[i] + " " + self.destDocs_vals[i] + "\n")

        #Scores
        count = 0
        file.write("--- Scores ---\n")
        for i in range(0, len(self.sourceDocs)):
            for j in range(0, len(self.destDocs)):
                file.write("{:.2f}".format(self.scores[i,j]) + " ")            
            # if(np.min(scores[i,:]) <= 0.2 and np.min(scores[i,:]) > 0.1):
            #     count = count + 1
            #     file.write("|Match|" + "{:.2f}".format(np.min(scores[i,:])) + "|")
            #     file.write(docs1_vals[i] + "//" + docs2_vals[np.argmin(scores[i,:])])
            file.write("\n")            
        
        # print(count)
        file.close()  

    #Generate dictionary of top k - similarity 
    def ToKDictionary(self, k, dir=""):

        print("Generating top k similarity dictionary between " + self.sourceName + " and " + self.destName)
        
        self.dict = {} #Clean the dictionary
        count = 0

        for i in range(0, len(self.sourceDocs)):
            result = {}
            result["Name"] = self.sourceDocs_vals[i]        
            matching = {}
            key_scores = {}            

            for j in range(0, len(self.destDocs)):
                key_scores[j] = self.scores[i, j]

            #Sort the score dict
            sorted_scores = sorted(key_scores, key=lambda x: key_scores[x])
            tmp = 0
            #Get top k nearest name
            for j in sorted_scores:
                data = {}
                data["Name"] = self.destDocs_vals[j]        
                data["Score"] = self.scores[i,j]
                matching[self.destDocs_keys[j]] = data
                tmp = tmp + 1
                if tmp >= k: 
                    break

            result["K-matching"] = matching
            self.dict[self.sourceDocs_keys[i]] = result

            count = count + 1
            if (count % 100 == 0):
                print("Calculated for " + str(count) + " instances")
            
        print("Calculated for " + str(count) + " instances")

        if dir == "":
            dir = self.sourceName + "_" + self.destName + "_top" + str(k)
        
        file = open(self.rootDir + dir, "w")          
        file.write(json.dumps(self.dict))
        file.close()  
    
    def loadGeneralOutput(self, dir = ""):
        if dir == "":
            dir = self.sourceName + "_" + self.destName          
        
        file = open(self.rootDir + dir, "r")          

        line = ""
        
        #Firstline - params
        line = file.readline()
        params = line.split()
        sourceCount = int(params[0])
        destCount = int(params[1])
        self.sourceDocs = {}
        self.destDocs = {} 
        self.sourceDocs_keys = [] 
        self.sourceDocs_vals = []        
        self.destDocs_keys = [] 
        self.destDocs_vals = []        
        
        #Source
        sourceDocs = {}
        line = file.readline() #Instruction line
        for i in range(0, sourceCount):
            line = file.readline()
            params = line.split()
            key = params[0]
            value = line[len(params[0]) + 1:-1]
            self.sourceDocs[key] = value
            self.sourceDocs_keys.append(key) 
            self.sourceDocs_vals.append(value)        
        
        #Dest
        destDocs = {}
        line = file.readline() #Instruction line
        for i in range(0, destCount):
            line = file.readline()
            params = line.split()
            key = params[0]
            value = line[len(params[0]) + 1:-1]
            self.destDocs[key] = value
            self.destDocs_keys.append(key)
            self.destDocs_vals.append(value)
        
        #Scores
        self.scores = np.zeros((len(self.sourceDocs), len(self.destDocs)))       
        line = file.readline() #Instruction line
        for i in range(0, sourceCount):
            line = file.readline()
            params = line.split()
            for j in range(0, destCount):
                self.scores[i, j] = float(params[j])
               
if __name__ == '__main__':

    config = {}
    config["HOST"] = "localhost"
    config["GATE"] = 27017
    config["DB_NAME"] = "google_review"

    linkage = LinkageSimilarity()
    linkage.setMongoConfig(config)
    linkage.setSourceName("Google_Hotels")
    linkage.setDestName("Yelp_Hotels")
    # linkage.setSourceDocs("Google_Hotels")
    # linkage.setDestDocs("Yelp_Hotels")
    # linkage.calculateScore()
    # linkage.filterBySimilarity(0.2)
    linkage.loadGeneralOutput()
    linkage.toGeneralOutput("output")