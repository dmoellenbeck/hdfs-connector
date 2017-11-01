package org.mule.extension.hdfs.internal.operation;

import org.mule.extension.hdfs.api.error.HdfsErrorType;
import org.mule.extension.hdfs.internal.service.exception.InvalidRequestDataException;
import org.mule.extension.hdfs.internal.service.exception.UnableToRetrieveResponseException;
import org.mule.extension.hdfs.internal.service.exception.UnableToSendRequestException;
import org.mule.runtime.extension.api.exception.ModuleException;

import java.util.function.Consumer;
import java.util.function.Supplier;

public abstract class BaseOperations {

    private static final String ERROR_CODE =" ERROR CODE: " ;

    protected  <T> T execute(Supplier<T> supplier) {
        try {
            return supplier.get();
        } catch (InvalidRequestDataException e) {
            throw new ModuleException(e.getMessage() + ERROR_CODE + e.getErrorCode(),
                    HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (UnableToSendRequestException | UnableToRetrieveResponseException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.CONNECTIVITY, e);
        } catch (IllegalArgumentException e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (Exception e) {
            throw new ModuleException(e.getMessage(), HdfsErrorType.UNKNOWN, e);
        }
    }

    protected void execute(Runnable runnable) {
        execute(() -> {
            runnable.run();
            return null;
        });
    }

}