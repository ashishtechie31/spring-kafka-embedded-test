package com.learning.kafka;

import com.learning.kafka.util.ComponentTestUtil;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.Method;

public class IntroduceDelayExtension implements BeforeTestExecutionCallback {

    @Override
    public void beforeTestExecution(ExtensionContext context) {
        Method testMethod = context.getRequiredTestMethod();
        IntroduceDelay annotation = testMethod.getAnnotation(IntroduceDelay.class);
        if (annotation != null) {
            int duration = annotation.duration();
            ComponentTestUtil.waitForExecution(duration);
        }
    }

}
