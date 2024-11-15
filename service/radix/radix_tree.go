package radix

type RadixNode struct {
	ID           int
	Weight       int
	Children     map[string]*RadixNode
	chileRuneMin int
	childRuneMax int
}

func (rn *RadixNode) calc_chile_rune_length() {
	min := 0
	max := 0
	for k, _ := range rn.Children {
		key_len := len([]rune(k))
		if min == 0 {
			min = key_len
		} else if key_len < min {
			min = key_len
		}
		if max == 0 {
			max = key_len
		} else if key_len > max {
			max = key_len
		}
	}
	rn.chileRuneMin = min
	rn.childRuneMax = max
}

func (rn *RadixNode) GetChildRuneMin() int {
	if rn.chileRuneMin > 0 {
		return rn.chileRuneMin
	}
	if len(rn.Children) == 0 {
		return 0
	}
	rn.calc_chile_rune_length()
	return rn.chileRuneMin
}

func (rn *RadixNode) GetChildRuneMax() int {
	if rn.childRuneMax > 0 {
		return rn.childRuneMax
	}
	if len(rn.Children) == 0 {
		return 0
	}
	rn.calc_chile_rune_length()
	return rn.childRuneMax
}
