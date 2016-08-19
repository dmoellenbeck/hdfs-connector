/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.utils;

import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Description;
import org.junit.internal.matchers.TypeSafeMatcher;

/**
 * @author MuleSoft, Inc.
 */
public class IsCausedBy extends TypeSafeMatcher<Throwable> {

    private final Class<? extends Throwable> type;
    private final String expectedMessage;

    public IsCausedBy(Class<? extends Throwable> type, String expectedMessage) {
        this.type = type;
        this.expectedMessage = expectedMessage;
    }

    @Override
    public boolean matchesSafely(Throwable item) {
        return item.getCause()
                .getClass()
                .isAssignableFrom(type)
                && item.getCause()
                        .getMessage()
                        .contains(expectedMessage);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("expects cause by type ")
                .appendValue(type);
        if (StringUtils.isNotEmpty(expectedMessage)) {
            description.appendText(" and a cause by message ")
                    .appendValue(expectedMessage);
        }
    }
}
