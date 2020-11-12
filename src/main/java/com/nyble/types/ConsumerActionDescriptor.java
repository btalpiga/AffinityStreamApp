package com.nyble.types;

import com.nyble.topics.consumerActions.ConsumerActionsValue;

import java.util.function.Function;

public class ConsumerActionDescriptor {
    private int id;
    private String description;
    private int score;
    private String systemId;
    private boolean complex;
    private Function<ConsumerActionsValue.ConsumerActionsPayload, Integer> complexScoreComputeFunction;

    public ConsumerActionDescriptor(int id, String description, int score, String systemId) {
        this.id = id;
        this.description = description;
        this.score = score;
        this.systemId = systemId;
    }

    public ConsumerActionDescriptor(int id, String description, String systemId, boolean complex,
                                    Function<ConsumerActionsValue.ConsumerActionsPayload, Integer> complexScoreComputeFunction) {
        this.id = id;
        this.description = description;
        this.score = 0;
        this.systemId = systemId;
        this.complex = complex;
        this.complexScoreComputeFunction = complexScoreComputeFunction;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public String getSystemId() {
        return systemId;
    }

    public void setSystemId(String systemId) {
        this.systemId = systemId;
    }

    public boolean isComplex() {
        return complex;
    }

    public Function<ConsumerActionsValue.ConsumerActionsPayload, Integer> getComplexScoreComputeFunction() {
        return complexScoreComputeFunction;
    }
}
