package com.ab.kkmallmqconsumer.exception;

import com.ab.kkmallmqconsumer.common.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(BusinessException.class)
    public Result<?> handleBusinessException(BusinessException exception) {
        log.error("业务异常: {}", exception.getMessage());
        return new Result<>(exception.getCode(), exception.getMessage(), null);
    }

    @ExceptionHandler(Exception.class)
    public Result<?> handleException(Exception exception) {
        log.error("系统异常", exception);
        return Result.error("系统错误，请稍后重试");
    }
}
