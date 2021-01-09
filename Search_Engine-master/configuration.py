class ConfigClass:
    def __init__(self):
        # link to a zip file in google drive with your pretrained model
        self._model_url = "https://drive.google.com/file/d/1Dn3_DzO1aY46H0N4xj3-nlH4QG5t3axR/view?usp=sharing"
        # False/True flag indicating whether the testing system will download 
        # and overwrite the existing model files. In other words, keep this as 
        # False until you update the model, submit with True to download 
        # the updated model (with a valid model_url), then turn back to False 
        # in subsequent submissions to avoid the slow downloading of the large 
        # model file with every submission.
        self._download_model = True

        self.corpusPath = ''
        self.savedFileMainFolder = ''
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
        self.toStem = False

        print('Project was created successfully..')

    def get__corpusPath(self):
        return self.corpusPath

    def get_model_url(self):
        return self._model_url

    def get_download_model(self):
        return self._download_model

    def set__corpusPath(self, corpusPath):
        self.corpusPath = corpusPath

    def set__outputPath(self, outputPath):
        self.savedFileMainFolder = outputPath

    def get__outputPath(self):
        return self.savedFileMainFolder
