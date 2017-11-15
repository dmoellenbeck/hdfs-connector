package org.mule.extension.hdfs.internal.error;

import org.mule.extension.hdfs.internal.service.exception.HdfsConnectionException;
import org.mule.extension.hdfs.internal.service.exception.HdfsException;
import org.mule.extension.hdfs.internal.service.exception.InvalidRequestDataException;


import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.extension.api.exception.ModuleException;
import org.mule.runtime.extension.api.runtime.exception.ExceptionHandler;

public class HdfsExceptionHandler extends ExceptionHandler {

    private static final String ERROR_CODE = " ERROR CODE: ";

    @Override
    public Exception enrichException(Exception exception) {
        try {
            throw exception;
        } catch (InvalidRequestDataException e) {
            return new ModuleException(e.getMessage() + ERROR_CODE + e.getErrorCode(), HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (HdfsConnectionException e) {
            return new ConnectionException(e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            return new ModuleException(e.getMessage(), HdfsErrorType.INVALID_REQUEST_DATA, e);
        } catch (HdfsException e) {
            return new ModuleException(e.getMessage(), HdfsErrorType.UNKNOWN, e);
        } catch (Exception e) {
            return new ModuleException(e.getMessage(), HdfsErrorType.UNKNOWN, e);
        }
    }
}
