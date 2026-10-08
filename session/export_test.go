package session

// SetAfterPinCheck sets a hook that placerFor runs between the pin check and the override lookup.
func (s *Service) SetAfterPinCheck(f func()) { s.afterPinCheck = f }
