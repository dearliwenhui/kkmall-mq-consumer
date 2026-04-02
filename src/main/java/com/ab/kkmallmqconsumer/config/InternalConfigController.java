package com.ab.kkmallmqconsumer.config;

import com.ab.kkmallmqconsumer.common.Result;
import com.ab.kkmallmqconsumer.model.CdcTopicRule;
import com.ab.kkmallmqconsumer.service.ConfigSnapshotService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/internal/config")
@RequiredArgsConstructor
public class InternalConfigController {

    private final ConfigSnapshotService configSnapshotService;

    @GetMapping("/topics")
    public Result<List<CdcTopicRule>> topics() {
        return Result.success(configSnapshotService.snapshot());
    }
}
