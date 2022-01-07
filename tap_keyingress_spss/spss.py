root_cols = [
        'i_NUMBER',
        'i_TAN',
        'i_TID',
        'i_START',
        'i_END',
        'i_TIME',
        'i_STATUS',
        'i_ST_TXT',
        'current_Q']

class InterviewCollector():

    def __init__(self, modified, source_file):
        self.modified = modified
        self.source_file = source_file

    def collectInterviews(self, df):
        df = df[root_cols]
        df["source_file"] = self.source_file
        df["file_modified"] = self.modified
        return df.to_dict(orient="records")

    def collectInterviewAnswers(self, df, meta):
        del_cols = root_cols.copy()
        del_cols.remove('i_NUMBER')
        df = df.drop(columns=del_cols)
        df = df.set_index("i_NUMBER").stack().reset_index()
        df = df.rename(columns={df.columns[1]:'question_id', df.columns[2]:'q_answer'})
        
        df["dtype"] = df["question_id"].apply(lambda x : self.__getDtype(x, meta))
        
        df.loc[df["dtype"]=="string","q_answer_text"] = df["q_answer"]
        df["q_answer_text"] = df["q_answer_text"].fillna('')
        df.loc[df["q_answer_text"].str.len() == 0,"q_answer_text"] = None
        
        df.loc[df["dtype"]=="number","q_answer_number"] = df["q_answer"]
        df.loc[df["q_answer_number"].isnull(),"q_answer_number"] = None
        
        df.drop(columns=["q_answer","dtype"], inplace=True)

        df["source_file"] = self.source_file
        df["file_modified"] = self.modified

        return df.to_dict(orient="records")

    def __getDtype(self, column, meta):
        otype = meta.original_variable_types.get(column)
        if otype is None:
            raise ValueError("something is wrong with the data types")
        if otype.startswith('F'):
            return "number"
        elif otype.startswith('A'):
            return "string"
        
        raise ValueError(f"unknown datatype {otype}")


class QuestionCollector():

    def __init__(self, df, meta, modified, source_file):
        self.df = df
        self.meta = meta
        self.modified = modified
        self.source_file = source_file
        self.questions = self.__collectQuestions()

    def getQuestions(self):
        q = []
        for qu in self.questions:
            qu = qu.copy()
            qu.pop("variable_labels")
            qu['file_modified'] = self.modified
            qu['source_file'] = self.source_file
            q.append(qu)
        return q

    def getOptions(self):
        q = []
        for i in self.questions:
            var_labels = i.get('variable_labels')
            if var_labels is not None and len(var_labels) > 0:
                for k, v in var_labels.items():
                    opt = {}
                    opt['question_id'] = i['question_id']
                    opt['option_id'] = k
                    opt['option_label'] = v
                    opt['file_modified'] = self.modified
                    opt['source_file'] = self.source_file
                    q.append(opt)
        return q

    def __collectQuestions(self):
        options2PlaceholderQ = self.__getOptions2PlaceholderQuestions()
        questions = []
        for c in self.meta.column_names:
            if c in root_cols:
                continue

            question = {}
            question['question_id'] = c
            question['question_text'] = self.meta.column_names_to_labels.get(c)
            question['measure'] = self.meta.variable_measure.get(c)
            question['q_datatype'] = self.meta.readstat_variable_types.get(c)
            question['original_type'] = self.meta.original_variable_types.get(c)
            question['variable_labels'] = self.meta.variable_value_labels.get(c)
            question['is_question_placeholder'] = self.__columnIsQuestionPlaceHolder(c)
            opq = options2PlaceholderQ.get(c)
            if opq is not None:
                question['parent_question_id'] = opq.get('parent_question')
                question['core_question'] = opq.get('core_question')
                question['option'] = opq.get('option')
            else:
                question['parent_question_id'] = None
                question['core_question'] = None
                question['option'] = None

            questions.append(question)

        return questions

    def __getOptions2PlaceholderQuestions(self):
        placeholder = []
        for c in self.meta.column_names:
            if self.__columnIsQuestionPlaceHolder(c):
                placeholder.append(c)

        options2PlaceholderQuestions = {}
        for ph in placeholder:
            startLooking = False
            for c in self.meta.column_names:
                if ph == c:
                    startLooking = True
                    continue
                if startLooking:
                    ph_core = ph.replace('_question','')
                    if ph_core not in c or c in placeholder:
                        break
                        
                    options2PlaceholderQuestions[c] = {'parent_question':ph,
                                                    'core_question': ph_core,
                                                    'option': c.replace(ph_core,'')}
            
        return options2PlaceholderQuestions

    def __columnIsQuestionPlaceHolder(self, c):
        if '_question' in c:
            return True
        if self.df[self.df[c].isnull() == False].shape[0] == 0:
            return True
        
        return False

