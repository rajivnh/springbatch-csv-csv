package com.ccs.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EmpUdbData {
	private Integer empNumber;
	
	private String empName;
	
	private String empAddr;
}
