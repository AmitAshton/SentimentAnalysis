package com.ashton.SentimentAnalysis.nlp;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentClass;
import edu.stanford.nlp.util.CoreMap;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Service
public class SentimentAnalyzer {
    StanfordCoreNLP nlp;
    Map<String, Double> sentimentValues = new HashMap<>();

    @PostConstruct
    public void init() {
        Properties nlpProps = new Properties();
        nlpProps.put("annotators", "tokenize, ssplit, parse, sentiment");
        nlp = new StanfordCoreNLP(nlpProps);
        sentimentValues.put("Very negative", 1d);
        sentimentValues.put("Negative", 2d);
        sentimentValues.put("Neutral",3d);
        sentimentValues.put("Positive",4d);
        sentimentValues.put("Very positive",5d);

    }

    public Double analyze(String text) {
        Annotation annotation = nlp.process(text);
        List<CoreMap> sentences =  annotation.get(CoreAnnotations.SentencesAnnotation.class);
        return   sentences.stream()
                .map(sentence->sentence.get(SentimentClass.class))
                .map(sentimentStr->sentimentValues.get(sentimentStr))
                .mapToDouble(x->x).sum()
                / (Double.parseDouble(String.valueOf(sentences.size())));
    }
}
