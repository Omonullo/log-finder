package uz.omonako.logfinder.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import uz.omonako.logfinder.service.LogService;

import java.util.List;

@RestController
public class RESTController {


    @Autowired
    private LogService logService;

    @PostMapping("/get-update-logs")
    public String getUpdateLogs(@RequestBody List<String> pinflList) {
        return logService.searchUpdateQueries(pinflList);
    }
}
